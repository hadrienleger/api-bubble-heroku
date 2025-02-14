/****************************************************
 * Fichier : api-bubble-heroku_v2_iris.js
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

// --------------------------------------------------------------
// A) Fonctions utilitaires (intersection, union, différence)
// ---------------------------------- ----------------------------
function intersectArrays(arrA, arrB) {
  const setB = new Set(arrB);
  return arrA.filter(x => setB.has(x));
}
function unionArrays(arrA, arrB) {
  const setA = new Set(arrA);
  for (const x of arrB) {
    setA.add(x);
  }
  return Array.from(setA);
}
function differenceArrays(arrA, arrB) {
  const setB = new Set(arrB);
  return arrA.filter(x => !setB.has(x));
}

// --------------------------------------------------------------
// B) Vérification d'activation des critères
// --------------------------------------------------------------
function isDVFActivated(dvf) {
  if (!dvf) return false;
  const hasType    = dvf.propertyTypes && dvf.propertyTypes.length>0;
  const hasBudget  = dvf.budget && (dvf.budget.min!=null || dvf.budget.max!=null);
  const hasSurface = dvf.surface && (dvf.surface.min!=null || dvf.surface.max!=null);
  const hasRooms   = dvf.rooms &&   (dvf.rooms.min!=null   || dvf.rooms.max!=null);
  const hasYears   = dvf.years &&   (dvf.years.min!=null   || dvf.years.max!=null);
  return (hasType || hasBudget || hasSurface || hasRooms || hasYears);
}
function isRevenusActivated(rev) {
  if (!rev) return false;
  if (rev.mediane_rev_decl && (rev.mediane_rev_decl.min!=null || rev.mediane_rev_decl.max!=null)) return true;
  return false;
}
function isLogSocActivated(ls) {
  if (!ls) return false;
  if (ls.part_log_soc && (ls.part_log_soc.min!=null || ls.part_log_soc.max!=null)) return true;
  return false;
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

// --------------------------------------------------------------
// C) Récupérer communes à partir de départements
// --------------------------------------------------------------
async function getCommunesFromDepartements(depCodes) {
  // renvoie la liste de communes pour un array de code départements
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

// Cette fonction peut être déclarée juste au-dessus de getIrisLocalisationAndInsecurite
async function gatherCommuneCodes(selectedLocalities) {
  let allCodes = [];

  for (let loc of selectedLocalities) {
    if (loc.type_collectivite === "Département") {
      // => Convertir code_insee (ex. "75") en liste de communes
      console.time(`getCommunesFromDep-${loc.code_insee}`);
      let result = await getCommunesFromDepartements([loc.code_insee]);
      console.timeEnd(`getCommunesFromDep-${loc.code_insee}`);
      allCodes.push(...result);
    } else {
      // type_collectivite = "commune" ou "arrondissement"
      allCodes.push(loc.code_insee);
    }
  }

  // Retirer doublons
  return Array.from(new Set(allCodes));
}


// --------------------------------------------------------------
// D) getIrisLocalisationAndInsecurite
//     1) Récup communes (type com/dep)
//     2) Filtre sur insécurité => communesFinal
//     3) Récup IRIS correspondants
// --------------------------------------------------------------

async function getIrisLocalisationAndInsecurite(params, insecu) {
  console.time('A) localiser communes');

  // 1) On vérifie que params.selected_localities existe et est un tableau
  if (!params.selected_localities || !Array.isArray(params.selected_localities)) {
    throw new Error('Paramètre "selected_localities" manquant ou invalide (doit être un array).');
  }

  // 2) Récupérer la liste unifiée de communes (arrondissements = communes, départements => on les éclate en communes)
  let communesSelection = await gatherCommuneCodes(params.selected_localities);
  console.log('=> communesSelection.length =', communesSelection.length);
  console.timeEnd('A) localiser communes');

  // 3) Si aucune commune n'est trouvée, on renvoie un objet vide
  if (!communesSelection.length) {
    return { arrayIrisLoc: [], communesFinal: [] };
  }

  // ----------------------------------------------------------------
  // I) insécurité => note >= insecu.min
  // ----------------------------------------------------------------
  let communesFinal = communesSelection;
  if (insecu && insecu.min != null) {
    console.time('B) insecurite query');
    const qInsecu = `
      SELECT insee_com
      FROM delinquance.notes_insecurite_geom_complet
      WHERE note_sur_20 >= $1
        AND insee_com = ANY($2)
    `;
    let resInsec = await pool.query(qInsecu, [insecu.min, communesFinal]);
    console.timeEnd('B) insecurite query');

    let communesInsecOk = resInsec.rows.map(r => r.insee_com);
    console.log('=> communesInsecOk.length =', communesInsecOk.length);

    console.time('B) intersection communes insecurite');
    communesFinal = intersectArrays(communesFinal, communesInsecOk);
    console.timeEnd('B) intersection communes insecurite');
    console.log('=> communesFinal (after insecurite) =', communesFinal.length);

    if (!communesFinal.length) {
      return { arrayIrisLoc: [], communesFinal: [] };
    }
  }

  // ----------------------------------------------------------------
  // II) Récupérer les IRIS => decoupages.iris_2022
  // ----------------------------------------------------------------
  console.time('C) iris_2022 query');
  const qIris = `
    SELECT code_iris
    FROM decoupages.iris_2022
    WHERE insee_com = ANY($1)
  `;
  let rIris = await pool.query(qIris, [communesFinal]);
  console.timeEnd('C) iris_2022 query');

  let arrayIrisLoc = rIris.rows.map(rr => rr.code_iris);
  console.log('=> arrayIrisLoc.length =', arrayIrisLoc.length);

  // 4) Renvoie l'ensemble IRIS
  return { arrayIrisLoc, communesFinal };
}


// --------------------------------------------------------------
// D) Filtrage DVF => intersection stricte
// --------------------------------------------------------------
async function applyDVF(arrayIrisLoc, dvfCriteria) {
  console.time('D) DVF: activation?');
  if (!isDVFActivated(dvfCriteria)) {
    console.timeEnd('D) DVF: activation?');
    return { irisSet: arrayIrisLoc, dvfCountByIris: {} };
  }
  console.timeEnd('D) DVF: activation?');

  console.time('D) DVF: build query');
  let whereClauses = [];
  let values = [];
  let idx = 1;

  whereClauses.push(`code_iris = ANY($${idx})`);
  values.push(arrayIrisLoc);
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
  console.timeEnd('D) DVF: build query');

  const wh = `WHERE ` + whereClauses.join(' AND ');
  const query = `
    SELECT code_iris, COUNT(*)::int AS nb_mut
    FROM dvf_filtre.dvf_simplifie
    ${wh}
    GROUP BY code_iris
  `;
  console.time('D) DVF: exec query');
  let res = await pool.query(query, values);
  console.timeEnd('D) DVF: exec query');

  console.log('=> DVF rowCount =', res.rowCount);

  let dvfCountByIris = {};
  let irisOK = [];
  for (let row of res.rows) {
    dvfCountByIris[row.code_iris] = Number(row.nb_mut);
    irisOK.push(row.code_iris);
  }

  console.time('D) DVF: intersection');
  let irisSet = intersectArrays(arrayIrisLoc, irisOK);
  console.timeEnd('D) DVF: intersection');
  console.log('=> after DVF intersectionSet.length =', irisSet.length);

  return { irisSet, dvfCountByIris };
}

// --------------------------------------------------------------
// E) Filtrage revenus déclarés => intersection stricte
// --------------------------------------------------------------
async function applyRevenus(irisList, revCriteria) {
  console.time('E) Revenus-declares: activation?');
  if (!isRevenusActivated(revCriteria)) {
    console.timeEnd('E) Revenus-declares: activation?');
    return { irisSet: irisList, revenusByIris: {} };
  }
  console.timeEnd('E) Revenus-declares: activation?');

  console.time('E) Revenus-declares: build query');
  let w = [];
  let v = [];
  let i = 1;
  w.push(`code_iris = ANY($${i})`);
  v.push(irisList);
  i++;

  if (revCriteria.mediane_rev_decl) {
    if (revCriteria.mediane_rev_decl.min != null) {
      w.push(`mediane_rev_decl >= $${i}`);
      v.push(revCriteria.mediane_rev_decl.min);
      i++;
    }
    if (revCriteria.mediane_rev_decl.max != null) {
      w.push(`mediane_rev_decl <= $${i}`);
      v.push(revCriteria.mediane_rev_decl.max);
      i++;
    }
  }
console.timeEnd('E) Revenus-declares: build query');

  const query = `
    SELECT code_iris, mediane_rev_decl
    FROM filosofi.rev_decl_hl_2021
    WHERE ${w.join(' AND ')}
  `;
  console.time('E) Revenus-declares: exec query');
  let r = await pool.query(query, v);
  console.timeEnd('E) Revenus-declares: exec query');

  console.log(`=> Revenus-declares rowCount = ${r.rowCount}`);

  let revenusByIris = {};
  let irisOK = [];
  for (let row of r.rows) {
    revenusByIris[row.code_iris] = {
      mediane_rev_decl: Number(row.mediane_rev_decl)
    };
    irisOK.push(row.code_iris);
  }

  console.time('E) Revenus-declares: intersection');
  let irisSet = intersectArrays(irisList, irisOK);
  console.timeEnd('E) Revenus-declares: intersection');
  console.log(`=> after Revenus-declares intersectionSet.length = ${irisSet.length}`);

  return { irisSet, revenusByIris };
}

// --------------------------------------------------------------
// F) Filtrage Logements sociaux => intersection stricte
// --------------------------------------------------------------
async function applyLogSoc(irisList, lsCriteria) {
  console.time('F)1) LogSoc: activation?');
  if (!isLogSocActivated(lsCriteria)) {
    console.timeEnd('F)1) LogSoc: activation?');
    return { irisSet: irisList, logSocByIris: {} };
  }
  console.timeEnd('F)1) LogSoc: activation?');

  console.time('F)1) LogSoc: build query');
  let w = [];
  let v = [];
  let i = 1;

  // Remplace "iris" si ta table a "code_iris" comme nom
  w.push(`code_iris = ANY($${i})`);
  v.push(irisList);
  i++;

  if (lsCriteria.part_log_soc) {
    if (lsCriteria.part_log_soc.min!=null) {
      w.push(`part_log_soc >= $${i}`);
      v.push(lsCriteria.part_log_soc.min);
      i++;
    }
    if (lsCriteria.part_log_soc.max!=null) {
      w.push(`part_log_soc <= $${i}`);
      v.push(lsCriteria.part_log_soc.max);
      i++;
    }
  }
  console.timeEnd('F)1) LogSoc: build query');

  const query = `
    SELECT code_iris, part_log_soc
    FROM filosofi.logements_sociaux_iris_hl_2021
    WHERE ${w.join(' AND ')}
  `;
  console.time('F)1) LogSoc: exec query');
  let r = await pool.query(query, v);
  console.timeEnd('F)1) LogSoc: exec query');

  console.log(`=> LogSoc rowCount = ${r.rowCount}`);

  let logSocByIris = {};
  let irisOK = [];
  for (let row of r.rows) {
    logSocByIris[row.code_iris] = { part_log_soc: Number(row.part_log_soc) };
    irisOK.push(row.code_iris);
  }

  console.time('F)1) LogSoc: intersection');
  let irisSet = intersectArrays(irisList, irisOK);
  console.timeEnd('F)1) LogSoc: intersection');
  console.log(`=> after LogSoc intersectionSet.length = ${irisSet.length}`);

  return { irisSet, logSocByIris };
}

// --------------------------------------------------------------
// H) Critère partiel Ecoles => intersection stricte si "in-scope"
// --------------------------------------------------------------
async function applyEcolesPartial(irisList, ecolesCrit) {
  console.time('G) Ecoles: activation?');
  if (!isEcolesActivated(ecolesCrit)) {
    console.timeEnd('G) Ecoles: activation?');
    // => non activé => on met "non-activé" pour tous
    let ecolesByIris = {};
    for (let iris of irisList) {
      ecolesByIris[iris] = "non-activé";
    }
    return ecolesByIris;
  }
  console.timeEnd('G) Ecoles: activation?');

  console.time('G) Ecoles: subset coverage');
  // Récup tous les IRIS couverts
  const qDistinct = `
    SELECT DISTINCT code_iris
    FROM education_ecoles.iris_rne_ipsecoles
  `;
  let rDistinct = await pool.query(qDistinct);
  let setCouvert = new Set(rDistinct.rows.map(rr => rr.code_iris));
  let subsetCouvert = irisList.filter(iris => setCouvert.has(iris));
  console.timeEnd('G) Ecoles: subset coverage');
  console.log('G) Ecoles => subsetCouvert.length =', subsetCouvert.length);

  // => Hors-scope
  let ecolesByIris = {};
  let subsetHors = differenceArrays(irisList, subsetCouvert);
  for (let iris of subsetHors) {
    ecolesByIris[iris] = "hors-scope";
  }

  // 2) intersection stricte sur subsetCouvert + IPS
  console.time('G) Ecoles: build query');
  let w = [];
  let v = [];
  let i=1;

  w.push(`code_iris = ANY($${i})`);
  v.push(subsetCouvert);
  i++;

  if (ecolesCrit.ips_min!=null) {
    w.push(`ips >= $${i}`);
    v.push(ecolesCrit.ips_min);
    i++;
  }
  if (ecolesCrit.ips_max!=null) {
    w.push(`ips <= $${i}`);
    v.push(ecolesCrit.ips_max);
    i++;
  }
  console.timeEnd('G) Ecoles: build query');

  const query = `
    SELECT code_iris, code_rne, ips
    FROM education_ecoles.iris_rne_ipsecoles
    WHERE ${w.join(' AND ')}
  `;

  console.time('G) Ecoles: exec query');
  let rPivot = await pool.query(query, v);
  console.timeEnd('G) Ecoles: exec query');
  console.log('=> EcolesPivot rowCount =', rPivot.rowCount);

  let irisFoundSet = new Set();
  let mapEcoles = {}; // code_iris => array d'obj { code_rne, ips }

  for (let row of rPivot.rows) {
    irisFoundSet.add(row.code_iris);
    if (!mapEcoles[row.code_iris]) {
      mapEcoles[row.code_iris] = [];
    }
    mapEcoles[row.code_iris].push({
      code_rne: row.code_rne,
      ips: Number(row.ips)
    });
  }

  // => IRIS couverts mais pas dans irisFoundSet => => intersection stricte => array vide
  let subsetFiltre = Array.from(irisFoundSet);

  for (let iris of subsetCouvert) {
    if (!irisFoundSet.has(iris)) {
      // => 0 écols => => intersection stricte => on exclut ou on met []
      // Cf. v1 => partiel => on mettait [] => IRIS est "hors" ce critère?
      // Adapte à ta logique. Je mets []:
      mapEcoles[iris] = [];
    }
  }

  // Remplir ecolesByIris
  for (let iris of subsetFiltre) {
    ecolesByIris[iris] = mapEcoles[iris];
  }
  for (let iris of subsetCouvert) {
    if (!irisFoundSet.has(iris)) {
      ecolesByIris[iris] = [];
    }
  }

  // => hors-scope on a déjà "hors-scope"
  console.time('G) Ecoles: end');
  console.timeEnd('G) Ecoles: end');
  return ecolesByIris;
}

// --------------------------------------------------------------
// I) Critère partiel Collèges => intersection stricte si in-scope
// --------------------------------------------------------------
async function applyCollegesPartial(irisList, colCrit) {
  console.time('H) Colleges: activation?');
  if (!isCollegesActivated(colCrit)) {
    console.timeEnd('H) Colleges: activation?');
    let collegesByIris = {};
    for (let iris of irisList) {
      collegesByIris[iris] = "non-activé";
    }
    return collegesByIris;
  }
  console.timeEnd('H) Colleges: activation?');

  console.time('H) Colleges: subset coverage');
  // ex. DEPS_MANQUANTS
  const DEPS_MANQUANTS = ['17','22','2A','29','2B','52','56'];

  const qCouv = `
    SELECT code_iris
    FROM decoupages.iris_2022
    WHERE code_iris = ANY($1)
      AND insee_dep <> ALL($2)
  `;
  let rCouv = await pool.query(qCouv, [irisList, DEPS_MANQUANTS]);
  let subsetCouvert = rCouv.rows.map(rr => rr.code_iris);
  console.timeEnd('H) Colleges: subset coverage');
  console.log('H) Colleges => subsetCouvert.length =', subsetCouvert.length);

  // hors-scope
  let collegesByIris = {};
  let subsetHors = differenceArrays(irisList, subsetCouvert);
  for (let iris of subsetHors) {
    collegesByIris[iris] = "hors-scope";
  }

  // 2) pivot
  console.time('H) Colleges: build query');
  let w = [];
  let v = [];
  let i=1;

  w.push(`code_iris = ANY($${i})`);
  v.push(subsetCouvert);
  i++;

  if (colCrit.valeur_figaro_min!=null) {
    w.push(`niveau_college_figaro >= $${i}`);
    v.push(colCrit.valeur_figaro_min);
    i++;
  }
  if (colCrit.valeur_figaro_max!=null) {
    w.push(`niveau_college_figaro <= $${i}`);
    v.push(colCrit.valeur_figaro_max);
    i++;
  }
  console.timeEnd('H) Colleges: build query');

  const qPivot = `
    SELECT code_iris, code_rne, niveau_college_figaro
    FROM education_colleges.iris_rne_niveaucolleges
    WHERE ${w.join(' AND ')}
  `;
  console.time('H) Colleges: exec query');
  let rCols = await pool.query(qPivot, v);
  console.timeEnd('H) Colleges: exec query');
  console.log('=> CollegesPivot rowCount =', rCols.rowCount);

  let irisFoundSet = new Set();
  let mapCols = {};

  for (let row of rCols.rows) {
    irisFoundSet.add(row.code_iris);
    if (!mapCols[row.code_iris]) {
      mapCols[row.code_iris] = [];
    }
    mapCols[row.code_iris].push({
      code_rne: row.code_rne,
      valeur_figaro: Number(row.niveau_college_figaro)
    });
  }

  for (let iris of subsetCouvert) {
    if (!irisFoundSet.has(iris)) {
      // => 0 => intersection stricte => array vide
      mapCols[iris] = [];
    }
  }

  // remplir collegesByIris
  for (let iris of subsetHors) {
    // déjà = hors-scope
  }
  for (let iris of subsetCouvert) {
    if (!mapCols[iris]) {
      collegesByIris[iris] = [];
    } else {
      collegesByIris[iris] = mapCols[iris];
    }
  }

  return collegesByIris;
}

// --------------------------------------------------------------
// J) gatherInsecuByIris => note d'insécurité de la commune
// --------------------------------------------------------------
async function gatherInsecuByIris(irisList) {
  if (!irisList.length) return {};

  console.time('Insecu details: query');
  const q = `
    SELECT i.code_iris, i.insee_com,
           c.nom AS nom_com,
           d.note_sur_20
    FROM decoupages.iris_2022 i
    LEFT JOIN decoupages.communes c
      ON (c.insee_com = i.insee_com OR c.insee_arm = i.insee_com)
    LEFT JOIN delinquance.notes_insecurite_geom_complet d
      ON d.insee_com = i.insee_com
    WHERE i.code_iris = ANY($1)
  `;
  let r = await pool.query(q, [irisList]);
  console.timeEnd('Insecu details: query');

  let insecuByIris = {};
  for (let row of r.rows) {
    // On stocke un array, comme en V1 (même si IRIS = 1 commune)
    insecuByIris[row.code_iris] = [{
      insee: row.insee_com,
      nom_com: row.nom_com || '(commune inconnue)',
      note: row.note_sur_20 != null ? Number(row.note_sur_20) : null
    }];
  }
  return insecuByIris;
}

// --------------------------------------------------------------
// K) groupByCommunes => agrégation par commune (optionnel)
// --------------------------------------------------------------
async function groupByCommunes(irisList, communesFinal) {
  // si on veut un bloc "communes" dans le JSON
  if (!irisList.length || !communesFinal.length) {
    return [];
  }

  console.time('I) Communes regroupement');
  const query = `
    WITH selected_iris AS (
      SELECT unnest($1::text[]) AS iris
    ),
    expanded AS (
      SELECT s.iris, i.insee_com
      FROM selected_iris s
      JOIN decoupages.iris_2022 i ON i.code_iris = s.iris
    )
    SELECT e.insee_com, c.nom AS nom_com,
           c.insee_dep, c.nom_dep,
           COUNT(*) AS nb_iris
    FROM expanded e
    JOIN decoupages.communes c
      ON (c.insee_com = e.insee_com OR c.insee_arm = e.insee_com)
    WHERE e.insee_com = ANY($2::text[])
    GROUP BY e.insee_com, c.nom, c.insee_dep, c.nom_dep
    ORDER BY nb_iris DESC
  `;
  let communesRes = await pool.query(query, [irisList, communesFinal]);
  console.timeEnd('I) Communes regroupement');

  console.log('=> Nombre de communes distinctes =', communesRes.rowCount);

  let communesData = communesRes.rows.map(row => ({
    insee_com: row.insee_com,
    nom_com: row.nom_com,
    insee_dep: row.insee_dep,
    nom_dep: row.nom_dep,
    nb_iris: Number(row.nb_iris)
  }));
  return communesData;
}

// --------------------------------------------------------------
// L) POST /get_iris_filtre
// --------------------------------------------------------------
app.post('/get_iris_filtre', async (req, res) => {
  console.log('>>> BODY RECEIVED FROM BUBBLE:', JSON.stringify(req.body, null, 2));
  console.log('=== START /get_iris_filtre ===');
  console.time('TOTAL /get_iris_filtre');

  try {
    const { params, criteria } = req.body;
    if (!params || !params.selected_localities) {
      console.timeEnd('TOTAL /get_iris_filtre');
      return res.status(400).json({ error: 'Paramètres de localisation manquants (selected_localities).' });
    }

    // 1) Localisation + insécurité => IRIS
    const { arrayIrisLoc, communesFinal } = await getIrisLocalisationAndInsecurite(params, criteria?.insecurite);

    if (!arrayIrisLoc.length) {
      console.timeEnd('TOTAL /get_iris_filtre');
      return res.json({ nb_iris: 0, iris: [], communes: [] });
    }

    // 2) DVF => intersection
    let { irisSet: irisAfterDVF, dvfCountByIris } = await applyDVF(arrayIrisLoc, criteria?.dvf);
    if (!irisAfterDVF.length) {
      console.timeEnd('TOTAL /get_iris_filtre');
      return res.json({ nb_iris: 0, iris: [], communes: [] });
    }

    // 3) Revenus => intersection
    let { irisSet: irisAfterRevenus, revenusByIris } = await applyRevenus(irisAfterDVF, criteria?.filosofi);
    if (!irisAfterRevenus.length) {
      console.timeEnd('TOTAL /get_iris_filtre');
      return res.json({ nb_iris: 0, iris: [], communes: [] });
    }

    // 4) Logements sociaux => intersection
    let { irisSet: irisAfterSoc, logSocByIris } = await applyLogSoc(irisAfterRevenus, criteria?.filosofi);
    if (!irisAfterSoc.length) {
      console.timeEnd('TOTAL /get_iris_filtre');
      return res.json({ nb_iris: 0, iris: [], communes: [] });
    }

    // 5) Ecoles => partiel
    let ecolesByIris = await applyEcolesPartial(irisAfterSoc, criteria?.ecoles);

    // 6) Collèges => partiel
    let collegesByIris = await applyCollegesPartial(irisAfterSoc, criteria?.colleges);

    // 7) Récupérer la note insécurité => gatherInsecuByIris
    let insecuByIris = await gatherInsecuByIris(irisAfterSoc);

    // 8) Construire le tableau final
    let irisFinalDetail = [];
    for (let iris of irisAfterSoc) {
      let dvf_count = dvfCountByIris[iris] || 0;
      let rev = revenusByIris[iris] || {};
      let soc = logSocByIris[iris] || {};
      let ecolesVal = ecolesByIris[iris] || [];
      let colsVal = collegesByIris[iris] || [];
      let insecuVal = insecuByIris[iris] || [];

      irisFinalDetail.push({
        code_iris: iris,
        dvf_count,
        mediane_rev_decl: (rev.mediane_rev_decl !== undefined) ? rev.mediane_rev_decl : null,
        part_log_soc: (soc.part_log_soc !== undefined) ? soc.part_log_soc : null,
        insecurite: insecuVal, // ex. [ {insee, nom_com, note} ]
        ecoles: ecolesVal,
        colleges: colsVal
      });
    }

    // 9) GroupBy communes si besoin
    let communesData = await groupByCommunes(irisAfterSoc, communesFinal);

    // 10) Réponse
    console.log('=> final irisAfterSoc.length =', irisAfterSoc.length);
    const finalResp = {
      nb_iris: irisAfterSoc.length,
      iris: irisFinalDetail,
      communes: communesData
    };

    console.timeEnd('TOTAL /get_iris_filtre');
    return res.json(finalResp);

  } catch (err) {
    console.error('Erreur dans /get_iris_filtre :', err);
    console.timeEnd('TOTAL /get_iris_filtre');
    return res.status(500).json({ error: err.message });
  }
});

// Lancement
const PORT = process.env.PORT || 3000;
app.listen(PORT, () => {
  console.log(`API v2 IRIS démarrée sur le port ${PORT}`);
});
