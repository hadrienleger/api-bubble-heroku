/****************************************************
 * Fichier : api-bubble-heroku.js
 ****************************************************/

const express = require('express');
const cors = require('cors');
const { Pool } = require('pg');

// ----------------------------------
// Charger .env si on n'est pas en production
// ----------------------------------
if (process.env.NODE_ENV !== 'production') {
  require('dotenv').config();
}

// ----------------------------------
// Connexion PG
// ----------------------------------
const pool = new Pool({
  connectionString: process.env.ZENMAP_DATABASE_URL,
  ssl: process.env.NODE_ENV === 'production'
    ? { rejectUnauthorized: false }
    : false
});

const app = express();
app.use(cors());
app.use(express.json());

// Petit test
app.get('/ping', (req, res) => {
  res.json({ message: 'pong', date: new Date() });
});

// ---------------------------------------------------------------------
// A) Fonctions utilitaires
// ---------------------------------------------------------------------
function intersectArrays(arrA, arrB) {
  const setB = new Set(arrB);
  return arrA.filter(x => setB.has(x));
}
function unionArrays(arrA, arrB) {
  const setA = new Set(arrA);
  for (const x of arrB) setA.add(x);
  return Array.from(setA);
}
function differenceArrays(arrA, arrB) {
  const setB = new Set(arrB);
  return arrA.filter(x => !setB.has(x));
}

// ---------------------------------------------------------------------
// B) Détection activation des critères
// ---------------------------------------------------------------------
function isDVFActivated(dvf) {
  if (!dvf) return false;
  const hasType    = dvf.propertyTypes && dvf.propertyTypes.length>0;
  const hasBudget  = dvf.budget && (dvf.budget.min!=null || dvf.budget.max!=null);
  const hasSurface = dvf.surface && (dvf.surface.min!=null || dvf.surface.max!=null);
  const hasRooms   = dvf.rooms &&   (dvf.rooms.min!=null   || dvf.rooms.max!=null);
  const hasYears   = dvf.years &&   (dvf.years.min!=null   || dvf.years.max!=null);
  return (hasType || hasBudget || hasSurface || hasRooms || hasYears);
}
function isFilosofiActivated(filo) {
  if (!filo) return false;
  const hasNv = filo.nv_moyen && (filo.nv_moyen.min!=null || filo.nv_moyen.max!=null);
  const hasPart = filo.part_log_soc && (filo.part_log_soc.min!=null || filo.part_log_soc.max!=null);
  return (hasNv || hasPart);
}
function isCollegesActivated(col) {
  if (!col) return false;
  if (col.valeur_figaro_min!=null || col.valeur_figaro_max!=null) return true;
  return false;
}
function isEcolesActivated(ec) {
  if (!ec) return false;
  if (ec.ips_min!=null || ec.ips_max!=null) return true;
  return false;
}

// ---------------------------------------------------------------------
// C) getCommunesFromDepartements
// ---------------------------------------------------------------------
async function getCommunesFromDepartements(depCodes) {
  let allCommunes = [];
  for (let dep of depCodes) {
    const query = `
      SELECT DISTINCT
        CASE
          WHEN (c.insee_com = '75056' OR c.insee_com = '69123' OR c.insee_com = '13055')
               AND c.insee_arm IS NOT NULL AND c.insee_arm <> ''
          THEN c.insee_arm
          ELSE c.insee_com
        END AS commune
      FROM decoupages.communes c
      WHERE c.insee_dep = $1
        AND c.insee_com IS NOT NULL
    `;
    console.time(`getCommunesFromDep-${dep}`);
    let result = await pool.query(query, [dep]);
    console.timeEnd(`getCommunesFromDep-${dep}`);

    let communesDep = result.rows.map(r => r.commune);
    allCommunes.push(...communesDep);
  }
  return Array.from(new Set(allCommunes));
}

// ---------------------------------------------------------------------
// D) getCarresLocalisationAndInsecurite
//     => retourne arrayCarreLoc ET communesFinal
// ---------------------------------------------------------------------
async function getCarresLocalisationAndInsecurite(params, criteria) {
  console.time('A) localiser communes');
  let communesSelection = [];

  if (params.code_type === 'com') {
    communesSelection = params.codes;
  }
  else if (params.code_type === 'dep') {
    let allCom = [];
    for (let dep of params.codes) {
      let comDep = await getCommunesFromDepartements([dep]);
      allCom.push(...comDep);
    }
    communesSelection = Array.from(new Set(allCom));
  }
  else {
    throw new Error('code_type doit être "com" ou "dep".');
  }

  console.log('=> communesSelection.length =', communesSelection.length);
  console.timeEnd('A) localiser communes');

  if (!communesSelection.length) {
    return { arrayCarreLoc: [], communesFinal: [] };
  }

  // B) insécurité
  let communesFinal = communesSelection;
  if (criteria?.insecurite?.min != null) {
    console.time('B) insecurite query');
    const qInsecu = `
      SELECT insee_com
      FROM delinquance.notes_insecurite_geom_complet
      WHERE note_sur_20 >= $1
        AND insee_com = ANY($2)
    `;
    let resInsec = await pool.query(qInsecu, [criteria.insecurite.min, communesFinal]);
    console.timeEnd('B) insecurite query');

    let communesInsecOk = resInsec.rows.map(r => r.insee_com);
    console.log('=> communesInsecOk.length =', communesInsecOk.length);

    console.time('B) intersection communes insecurite');
    communesFinal = intersectArrays(communesFinal, communesInsecOk);
    console.timeEnd('B) intersection communes insecurite');
    console.log('=> communesFinal (after insecurite) =', communesFinal.length);

    if (!communesFinal.length) {
      return { arrayCarreLoc: [], communesFinal: [] };
    }
  }

  // C) grille200m => arrayCarreLoc
  console.time('C) grille200m query');
  const qGrille = `
    SELECT idinspire AS id_carre_200m
    FROM decoupages.grille200m_metropole
    WHERE insee_com && $1
  `;
  let resCar = await pool.query(qGrille, [communesFinal]);
  console.timeEnd('C) grille200m query');

  let arrayCarreLoc = resCar.rows.map(r => r.id_carre_200m);
  console.log('=> arrayCarreLoc.length =', arrayCarreLoc.length);

  return { arrayCarreLoc, communesFinal };
}

// ---------------------------------------------------------------------
// E) applyDVF => intersection stricte + stockage dvfCountByCarre
// ---------------------------------------------------------------------
async function applyDVF(arrayCarreLoc, dvfCriteria, dvfCountByCarre) {
  // dvfCountByCarre : un objet passé en param pour le remplir
  if (!isDVFActivated(dvfCriteria)) {
    return arrayCarreLoc;
  }
  console.time('D) DVF build query');
  let whereClauses = [];
  let values = [];
  let idx = 1;

  whereClauses.push(`id_carre_200m = ANY($${idx})`);
  values.push(arrayCarreLoc);
  idx++;

  if (dvfCriteria.propertyTypes && dvfCriteria.propertyTypes.length>0) {
    whereClauses.push(`codtyploc = ANY($${idx})`);
    values.push(dvfCriteria.propertyTypes);
    idx++;
  }
  if (dvfCriteria.budget) {
    if (dvfCriteria.budget.min!=null) {
      whereClauses.push(`valeurfonc >= $${idx}`);
      values.push(dvfCriteria.budget.min);
      idx++;
    }
    if (dvfCriteria.budget.max!=null) {
      whereClauses.push(`valeurfonc <= $${idx}`);
      values.push(dvfCriteria.budget.max);
      idx++;
    }
  }
  if (dvfCriteria.surface) {
    if (dvfCriteria.surface.min!=null) {
      whereClauses.push(`sbati >= $${idx}`);
      values.push(dvfCriteria.surface.min);
      idx++;
    }
    if (dvfCriteria.surface.max!=null) {
      whereClauses.push(`sbati <= $${idx}`);
      values.push(dvfCriteria.surface.max);
      idx++;
    }
  }
  if (dvfCriteria.rooms) {
    if (dvfCriteria.rooms.min!=null) {
      whereClauses.push(`nbpprinc >= $${idx}`);
      values.push(dvfCriteria.rooms.min);
      idx++;
    }
    if (dvfCriteria.rooms.max!=null) {
      whereClauses.push(`nbpprinc <= $${idx}`);
      values.push(dvfCriteria.rooms.max);
      idx++;
    }
  }
  if (dvfCriteria.years) {
    if (dvfCriteria.years.min!=null) {
      whereClauses.push(`anneemut >= $${idx}`);
      values.push(dvfCriteria.years.min);
      idx++;
    }
    if (dvfCriteria.years.max!=null) {
      whereClauses.push(`anneemut <= $${idx}`);
      values.push(dvfCriteria.years.max);
      idx++;
    }
  }
  console.timeEnd('D) DVF build query');

  const wh = `WHERE ` + whereClauses.join(' AND ');
  const query = `
    SELECT id_carre_200m, COUNT(*)::int AS nb_mut
    FROM dvf_filtre.dvf_simplifie
    ${wh}
    GROUP BY id_carre_200m
  `;
  console.time('D) DVF exec query');
  let res = await pool.query(query, values);
  console.timeEnd('D) DVF exec query');

  // remplir dvfCountByCarre
  let idsDVF = [];
  for (let row of res.rows) {
    let id = row.id_carre_200m;
    let nb = Number(row.nb_mut);
    dvfCountByCarre[id] = nb;
    idsDVF.push(id);
  }
  console.log('=> DVF rowCount =', idsDVF.length);

  console.time('D) DVF intersection');
  let result = intersectArrays(arrayCarreLoc, idsDVF);
  console.timeEnd('D) DVF intersection');
  console.log('=> after DVF intersectionSet.length =', result.length);

  return result;
}

// ---------------------------------------------------------------------
// F) applyFilosofi => intersection stricte + stockage { nv_moyen, part_log_soc }
// ---------------------------------------------------------------------
async function applyFilosofi(arrayCarreLoc, filo, filoByCarre) {
  if (!isFilosofiActivated(filo)) {
    return arrayCarreLoc;
  }
  console.time('E) Filosofi build query');
  let whereClauses = [];
  let values = [];
  let idx = 1;

  whereClauses.push(`idcar_200m = ANY($${idx})`);
  values.push(arrayCarreLoc);
  idx++;

  if (filo.nv_moyen) {
    if (filo.nv_moyen.min != null) {
      whereClauses.push(`nv_moyen >= $${idx}`);
      values.push(filo.nv_moyen.min);
      idx++;
    }
    if (filo.nv_moyen.max != null) {
      whereClauses.push(`nv_moyen <= $${idx}`);
      values.push(filo.nv_moyen.max);
      idx++;
    }
  }
  if (filo.part_log_soc) {
    if (filo.part_log_soc.min!=null) {
      whereClauses.push(`part_log_soc >= $${idx}`);
      values.push(filo.part_log_soc.min);
      idx++;
    }
    if (filo.part_log_soc.max!=null) {
      whereClauses.push(`part_log_soc <= $${idx}`);
      values.push(filo.part_log_soc.max);
      idx++;
    }
  }
  const wh = `WHERE ` + whereClauses.join(' AND ');
  console.timeEnd('E) Filosofi build query');

  const q = `
    SELECT idcar_200m, nv_moyen, part_log_soc
    FROM filosofi.c200_france_2019
    ${wh}
  `;
  console.time('E) Filosofi exec query');
  let res = await pool.query(q, values);
  console.timeEnd('E) Filosofi exec query');

  let idsFilo = [];
  for (let row of res.rows) {
    let id = row.idcar_200m;
    filoByCarre[id] = {
      nv_moyen: Number(row.nv_moyen),
      part_log_soc: Number(row.part_log_soc)
    };
    idsFilo.push(id);
  }
  console.log('=> Filosofi rowCount =', idsFilo.length);

  console.time('E) Filosofi intersection');
  let result = intersectArrays(arrayCarreLoc, idsFilo);
  console.timeEnd('E) Filosofi intersection');
  console.log('=> after Filosofi intersectionSet.length =', result.length);

  return result;
}

// ---------------------------------------------------------------------
// G) Critère partiel Ecoles => ecolesByCarre
// ---------------------------------------------------------------------
async function applyEcolesPartial(arrayCarreLoc, ecolesCrit, ecolesByCarre) {
  if (!isEcolesActivated(ecolesCrit)) {
    // On ne remplit rien => par défaut "ecolesByCarre[id] = 'non activé'" ?
    // Ou on laisse undefined
    return arrayCarreLoc;
  }
  console.time('G) Ecoles');

  // 1) subset Paris
  console.time('G) Ecoles subsetCouvert query');
  const qParis = `
    SELECT idinspire
    FROM decoupages.grille200m_metropole
    WHERE idinspire = ANY($1)
      AND EXISTS (
        SELECT 1 FROM unnest(insee_com) c
        WHERE c ILIKE '751%'
      )
  `;
  let rParis = await pool.query(qParis, [arrayCarreLoc]);
  console.timeEnd('G) Ecoles subsetCouvert query');

  let subsetCouvert = rParis.rows.map(r => r.idinspire);
  console.log('G) Ecoles => subsetCouvert.length =', subsetCouvert.length);

  // 2) Si subsetCouvert = 0 => tout le monde est "hors-scope"
  if (!subsetCouvert.length) {
    // On met ecolesByCarre[id] = "hors-scope" pour tous
    for (let id of arrayCarreLoc) {
      ecolesByCarre[id] = "hors-scope";
    }
    console.timeEnd('G) Ecoles');
    return arrayCarreLoc;
  }

  // 3) Filtrer subsetCouvert via pivot + ips
  let wE = [];
  let vE = [];
  let idx = 1;

  wE.push(`id_carre_200m = ANY($${idx})`);
  vE.push(subsetCouvert);
  idx++;

  if (ecolesCrit.ips_min!=null) {
    wE.push(`ips >= $${idx}`);
    vE.push(ecolesCrit.ips_min);
    idx++;
  }
  if (ecolesCrit.ips_max!=null) {
    wE.push(`ips <= $${idx}`);
    vE.push(ecolesCrit.ips_max);
    idx++;
  }

  // Requête pivot
  const qEco = `
    SELECT id_carre_200m, code_rne, ips
    FROM education_ecoles.idcar200m_rne_ipsecoles
    WHERE ${wE.join(' AND ')}
  `;
  console.time('G) Ecoles pivot query');
  let rEco = await pool.query(qEco, vE);
  console.timeEnd('G) Ecoles pivot query');

  // => On a (id_carre_200m, code_rne, ips). On veut le nom de l'école.
  //   On fait un 2e mini-join sur la table "liste_etab" (adapte le nom):
  //   ex. SELECT code_rne, appellation_officielle, adresse_uai, code_postal_uai, libcommune_uai
  //   FROM education.liste_etab
  //   WHERE code_rne in (distinct list)

  // 3.b) Récupérer tous les code_rne => set
  let allRNE = new Set();
  for (let row of rEco.rows) {
    allRNE.add(row.code_rne);
  }
  let listRNE = Array.from(allRNE);

  // Si on a besoin de noms
  let nomEcolesByRNE = {};  // rne => { nom_ecole, adrs, ... }
  if (listRNE.length) {
    const placeholders = listRNE.map((_,i)=>`$${i+1}`).join(',');
    const qNom = `
      SELECT code_rne, appellation_officielle, adresse_uai, code_postal_uai, libelle_commune
      FROM education.liste_etab
      WHERE code_rne = ANY($1)
    `;
    let rNom = await pool.query(qNom, [listRNE]);
    for (let row of rNom.rows) {
      let rne = row.code_rne;
      nomEcolesByRNE[rne] = {
        nom_ecole: row.appellation_officielle,
        adresse: `${row.adresse_uai} ${row.code_postal_uai} ${row.libelle_commune}`
      };
    }
  }

  // 4) On regroupe par idCarre
  //    ecolesByCarre[id] = array d'objets { code_rne, nom_ecole, ips, ... }
  let subsetCouvertFiltreSet = new Set(); // pour l'intersection partielle

  for (let row of rEco.rows) {
    let id = row.id_carre_200m;
    subsetCouvertFiltreSet.add(id);

    if (!ecolesByCarre[id] || ecolesByCarre[id]==="hors-scope") {
      ecolesByCarre[id] = [];
    }
    let rne = row.code_rne;
    let info = nomEcolesByRNE[rne] || {};
    ecolesByCarre[id].push({
      code_rne: rne,
      nom_ecole: info.nom_ecole || '(nom inconnu)',
      adresse: info.adresse || '',
      ips: Number(row.ips)
    });
  }

  // => tous les carreaux dans subsetCouvert, mais pas dans subsetCouvertFiltreSet,
  //    reçoivent un tableau vide ? ou "count=0" ? A priori, c’est un “0 correspondances” possible.
  //    => Mais la doc partielle dit : on conserve le carreau hors intersection, 
  //       => sauf qu'il n'a pas d'école répondant aux min..max d'IPS.
  // on met ecolesByCarre[id] = [] si c'est dans subsetCouvert, 
  //    ou "hors-scope" si c'est hors subsetCouvert

  for (let id of subsetCouvert) {
    if (!subsetCouvertFiltreSet.has(id)) {
      // => 0 correspondances => ecolesByCarre[id] = []
      ecolesByCarre[id] = [];
    }
  }
  for (let id of arrayCarreLoc) {
    if (!subsetCouvert.includes(id)) {
      // => hors scope => ecolesByCarre[id] = "hors-scope"
      ecolesByCarre[id] = "hors-scope";
    }
  }

  // 5) Calcul final union
  let subsetCouvertFiltre = Array.from(subsetCouvertFiltreSet);
  let subsetHors = differenceArrays(arrayCarreLoc, subsetCouvert);
  let result = unionArrays(subsetCouvertFiltre, subsetHors);

  console.log(`G) Ecoles => result.length = ${result.length}`);
  console.timeEnd('G) Ecoles');
  return result;
}

// ---------------------------------------------------------------------
// H) Critère partiel Collèges => collegesByCarre
// ---------------------------------------------------------------------
async function applyCollegesPartial(arrayCarreLoc, colCrit, collegesByCarre) {
  if (!isCollegesActivated(colCrit)) {
    return arrayCarreLoc;
  }
  console.time('F) Colleges');

  // departements manquants
  const DEPS_MANQUANTS = ['17','22','2A','29','2B','52','56'];

  console.time('F) Colleges subsetCouvert query');
  const qDep = `
    SELECT idinspire
    FROM decoupages.grille200m_metropole
    WHERE idinspire = ANY($1)
      AND NOT EXISTS (
        SELECT 1
        FROM unnest(insee_dep) d
        WHERE d = ANY($2)
      )
  `;
  let rCouv = await pool.query(qDep, [arrayCarreLoc, DEPS_MANQUANTS]);
  console.timeEnd('F) Colleges subsetCouvert query');

  let subsetCouvert = rCouv.rows.map(r => r.idinspire);
  console.log('F) Colleges => subsetCouvert.length =', subsetCouvert.length);

  if (!subsetCouvert.length) {
    // => tout est hors-scope
    for (let id of arrayCarreLoc) {
      collegesByCarre[id] = "hors-scope";
    }
    console.timeEnd('F) Colleges');
    return arrayCarreLoc;
  }

  // 2) table pivot + figaro
  let wCols = [];
  let vals = [];
  let iC = 1;

  wCols.push(`id_carre_200m = ANY($${iC})`);
  vals.push(subsetCouvert);
  iC++;

  if (colCrit.valeur_figaro_min != null) {
    wCols.push(`niveau_college_figaro >= $${iC}`);
    vals.push(colCrit.valeur_figaro_min);
    iC++;
  }
  if (colCrit.valeur_figaro_max != null) {
    wCols.push(`niveau_college_figaro <= $${iC}`);
    vals.push(colCrit.valeur_figaro_max);
    iC++;
  }

  const qPivot = `
    SELECT DISTINCT id_carre_200m, code_rne, niveau_college_figaro
    FROM education_colleges.idcar200m_rne_niveaucolleges
    WHERE ${wCols.join(' AND ')}
  `;
  console.time('F) Colleges pivot query');
  let rCols = await pool.query(qPivot, vals);
  console.timeEnd('F) Colleges pivot query');

  // 2b) Récupérer noms => table "education.liste_etab" (à adapter)
  let setRNE = new Set();
  for (let row of rCols.rows) {
    setRNE.add(row.code_rne);
  }
  let arrRNE = Array.from(setRNE);

  let nomCollegeByRNE = {};
  if (arrRNE.length) {
    const qNom = `
      SELECT code_rne, appellation_officielle, adresse_uai, code_postal_uai, libelle_commune
      FROM education.liste_etab
      WHERE code_rne = ANY($1)
    `;
    let rcNom = await pool.query(qNom, [arrRNE]);
    for (let row of rcNom.rows) {
      nomCollegeByRNE[row.code_rne] = {
        nom_college: row.appellation_officielle,
        adresse: `${row.adresse_uai} ${row.code_postal_uai} ${row.libelle_commune}`
      };
    }
  }

  // 3) On regroupe
  let setCovFilt = new Set();
  for (let row of rCols.rows) {
    let id = row.id_carre_200m;
    setCovFilt.add(id);

    if (!collegesByCarre[id] || collegesByCarre[id]==="hors-scope") {
      collegesByCarre[id] = [];
    }
    let info = nomCollegeByRNE[row.code_rne] || {};
    collegesByCarre[id].push({
      code_rne: row.code_rne,
      nom_college: info.nom_college || '(nom inconnu)',
      adresse: info.adresse || '',
      valeur_figaro: Number(row.niveau_college_figaro)
    });
  }

  // 4) pour subsetCouvert - setCovFilt => [] = 0 correspondances
  let subsetCovFilt = Array.from(setCovFilt);
  for (let id of subsetCouvert) {
    if (!setCovFilt.has(id)) {
      collegesByCarre[id] = [];
    }
  }
  // 5) hors subset => "hors-scope"
  let subsetHors = differenceArrays(arrayCarreLoc, subsetCouvert);
  for (let id of subsetHors) {
    collegesByCarre[id] = "hors-scope";
  }

  // 6) union => final array
  let result = unionArrays(subsetCovFilt, subsetHors);
  console.log(`F) Colleges => result.length = ${result.length}`);

  console.timeEnd('F) Colleges');
  return result;
}

// ---------------------------------------------------------------------
// I) Récupérer la note d'insécurité détaillée : insecuByCarre
//     => un array [ { insee, nom_com, note } ... ] 
//    En admettant qu'un carreau recouvre 2 communes, on aura 2 entrées
// ---------------------------------------------------------------------
async function gatherInsecuDetails(intersectionSet) {
  // On fait un unnest(insee_com), left join sur la table delinquance
  // + decoupages.communes pour avoir le "nom_com" si besoin
  if (!intersectionSet.length) return {};

  // On va faire un "expanded" comme pour le regroupement communal
  const query = `
    WITH selected_ids AS (
      SELECT unnest($1::text[]) AS id
    ),
    expanded AS (
      SELECT g.idinspire AS id_carre, unnest(g.insee_com) AS insee
      FROM decoupages.grille200m_metropole g
      JOIN selected_ids s ON g.idinspire = s.id
    )
    SELECT e.id_carre, e.insee,
           c.nom AS nom_com,
           d.note_sur_20
    FROM expanded e
    LEFT JOIN decoupages.communes c 
       ON (c.insee_com = e.insee OR c.insee_arm = e.insee)
    LEFT JOIN delinquance.notes_insecurite_geom_complet d
       ON d.insee_com = e.insee
  `;
  let res = await pool.query(query, [intersectionSet]);

  let insecuByCarre = {};
  for (let row of res.rows) {
    let id = row.id_carre;
    if (!insecuByCarre[id]) {
      insecuByCarre[id] = [];
    }
    // note_sur_20 peut être null => communes non notées
    if (row.insee) {
      insecuByCarre[id].push({
        insee: row.insee,
        nom_com: row.nom_com || '(commune inconnue)',
        note: row.note_sur_20 != null ? Number(row.note_sur_20) : null
      });
    }
  }
  return insecuByCarre;
}

// ---------------------------------------------------------------------
// J) POST /get_carreaux_filtre
// ---------------------------------------------------------------------
app.post('/get_carreaux_filtre', async (req, res) => {
  console.log('>>> BODY RECEIVED FROM BUBBLE:', JSON.stringify(req.body, null, 2));
  console.log('=== START /get_carreaux_filtre ===');
  console.time('TOTAL /get_carreaux_filtre');

  try {
    const { params, criteria } = req.body;
    if (!params || !params.code_type || !params.codes) {
      console.timeEnd('TOTAL /get_carreaux_filtre');
      return res.status(400).json({ error: 'Paramètres de localisation manquants.' });
    }

    // 0) On initialise les maps
    let dvfCountByCarre = {};    // id => nb
    let filoByCarre = {};        // id => { nv_moyen, part_log_soc }
    let ecolesByCarre = {};      // id => array ou "hors-scope"
    let collegesByCarre = {};    // id => array ou "hors-scope"

    // 1) Localisation + insécurité
    const { arrayCarreLoc, communesFinal } = await getCarresLocalisationAndInsecurite(params, criteria);
    if (!arrayCarreLoc.length) {
      console.timeEnd('TOTAL /get_carreaux_filtre');
      return res.json({ nb_carreaux: 0, carreaux: [] });
    }

    // 2) DVF => intersection stricte + dvfCount
    let newSet = await applyDVF(arrayCarreLoc, criteria?.dvf, dvfCountByCarre);
    if (!newSet.length) {
      console.timeEnd('TOTAL /get_carreaux_filtre');
      return res.json({ nb_carreaux: 0, carreaux: [] });
    }

    // 3) Filosofi => intersection stricte + {nv_moyen, part_log_soc}
    newSet = await applyFilosofi(newSet, criteria?.filosofi, filoByCarre);
    if (!newSet.length) {
      console.timeEnd('TOTAL /get_carreaux_filtre');
      return res.json({ nb_carreaux: 0, carreaux: [] });
    }

    // 4) Collèges => partiel
    newSet = await applyCollegesPartial(newSet, criteria?.colleges, collegesByCarre);
    // => pas de check si 0 => on garde hors-scope / ou array vide

    // 5) Écoles => partiel
    newSet = await applyEcolesPartial(newSet, criteria?.ecoles, ecolesByCarre);

    // 6) newSet est la liste "finale" de carreaux
    const intersectionSet = newSet;
    console.log('=> final intersectionSet.length =', intersectionSet.length);
    if (!intersectionSet.length) {
      console.timeEnd('TOTAL /get_carreaux_filtre');
      return res.json({ nb_carreaux: 0, carreaux: [] });
    }

    // 7) Récupération des notes d'insécurité (détaillées) => insecuByCarre
    //    On fait ça même si critère insécurité pas activé, 
    //    histoire d'afficher la note commune ? A toi de voir
    const insecuByCarre = await gatherInsecuDetails(intersectionSet);

    // 8) Construire la liste "carreaux" + communes
    //    On avait déjà la "communesFinal" => si tu veux faire un tri 
    //    ou un regroupement final... 
    //    (Tu peux garder l'ancien "I) Communes regroupement" si besoin.)

    // Mais si tu veux la même logique, on fait un "expanded" unnest(insee_com) + group by => 
    //   comme ton ancien code. On skip si tu veux ?

    // 8b) On construit simplement un tableau complet
    let carreauxDetail = [];
    for (let id of intersectionSet) {
      let dvfCount = dvfCountByCarre[id] || 0;
      let filo = filoByCarre[id] || {};
      let ecolesVal = ecolesByCarre[id];     // array ou 'hors-scope'
      let colVal    = collegesByCarre[id];   // array ou 'hors-scope'
      let insecuVal = insecuByCarre[id] || [];

      carreauxDetail.push({
        id_carre_200m: id,
        dvf_count: dvfCount,
        nv_moyen: filo.nv_moyen || null,
        part_log_soc: filo.part_log_soc || null,
        insecurite: insecuVal,   // ex. [ {insee, nom_com, note}, ...]
        ecoles: ecolesVal,
        colleges: colVal
      });
    }

    // 8c) Communes regroument (ancienne partie "I)")
    console.time('I) Communes regroupement');

  // Si communesFinal.length = 0, c'est qu'il n'y avait plus de commune après insécurité
  if (!communesFinal.length) {
    // => On renvoie un tableau communes vide
    console.log('Pas de communesFinal => communesData = []');
    var communesData = [];
    console.timeEnd('I) Communes regroupement');

  } else {
    // Sinon on fait la requête d'agrégation

    const queryCommunes = `
      WITH selected_ids AS (
        SELECT unnest($1::text[]) AS id
      ),
      expanded AS (
        SELECT unnest(g.insee_com) AS insee
        FROM decoupages.grille200m_metropole g
        JOIN selected_ids s ON g.idinspire = s.id
      )
      SELECT
        e.insee AS insee_com,
        c.nom AS nom_com,
        c.insee_dep,
        c.nom_dep,
        COUNT(*) AS nb_carreaux
      FROM expanded e
      JOIN decoupages.communes c
        ON ( c.insee_com = e.insee OR c.insee_arm = e.insee )
      WHERE e.insee = ANY($2::text[])
      GROUP BY e.insee, c.nom, c.insee_dep, c.nom_dep
      ORDER BY nb_carreaux DESC
    `;

    // On envoie 2 paramètres:
    //  - $1 = intersectionSet (carreaux finaux)
    //  - $2 = communesFinal (les communes filtrées)
    let communesRes = await pool.query(queryCommunes, [
      intersectionSet,
      communesFinal
    ]);

    console.timeEnd('I) Communes regroupement');
    console.log('=> Nombre de communes distinctes =', communesRes.rowCount);

    var communesData = communesRes.rows.map(row => ({
      insee_com : row.insee_com,
      nom_com   : row.nom_com,
      insee_dep : row.insee_dep,
      nom_dep   : row.nom_dep,
      nb_carreaux: Number(row.nb_carreaux)
    }));
  }

    // 9) Réponse
    const finalResp = {
      nb_carreaux: intersectionSet.length,
      carreaux: carreauxDetail
      communes: communesData      // agrégation par commune
    };

    console.timeEnd('TOTAL /get_carreaux_filtre');
    return res.json(finalResp);

  } catch (err) {
    console.error('Erreur dans /get_carreaux_filtre :', err);
    console.timeEnd('TOTAL /get_carreaux_filtre');
    return res.status(500).json({ error: err.message });
  }
});

// Lancement
const PORT = process.env.PORT || 3000;
app.listen(PORT, () => {
  console.log(`API démarrée sur le port ${PORT}`);
});
