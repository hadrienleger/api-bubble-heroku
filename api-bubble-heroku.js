/****************************************************
 * Fichier : api-bubble-heroku.js
 * 
 * Usage local :
 *   1) npm install
 *   2) npm start
 * Déploiement Heroku :
 *   - Soit via Procfile : "web: node api-bubble-heroku.js"
 *   - Soit via script "start": "node api-bubble-heroku.js" dans package.json
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

// ----------------------------------
// Setup Express
// ----------------------------------
const app = express();
app.use(cors());
app.use(express.json());

// Petit endpoint de test
app.get('/ping', (req, res) => {
  res.json({ message: 'pong', date: new Date() });
});

// ---------------------------------------------------------------------
// 1) Fonctions utilitaires pour arrays
// ---------------------------------------------------------------------
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

// ---------------------------------------------------------------------
// 2) Détection de l'activation des critères
// ---------------------------------------------------------------------
function isDVFActivated(dvf) {
  if (!dvf) return false;
  const hasPropertyTypes = dvf.propertyTypes && dvf.propertyTypes.length > 0;
  const hasBudget = dvf.budget && (dvf.budget.min != null || dvf.budget.max != null);
  const hasSurface = dvf.surface && (dvf.surface.min != null || dvf.surface.max != null);
  const hasRooms = dvf.rooms && (dvf.rooms.min != null || dvf.rooms.max != null);
  const hasYears = dvf.years && (dvf.years.min != null || dvf.years.max != null);
  return (hasPropertyTypes || hasBudget || hasSurface || hasRooms || hasYears);
}
function isFilosofiActivated(filo) {
  if (!filo) return false;
  const hasNv = filo.nv_moyen && (filo.nv_moyen.min != null || filo.nv_moyen.max != null);
  const hasPart = filo.part_log_soc && (filo.part_log_soc.min != null || filo.part_log_soc.max != null);
  return (hasNv || hasPart);
}
function isCollegesActivated(col) {
  if (!col) return false;
  if (col.valeur_figaro_min != null || col.valeur_figaro_max != null) {
    return true;
  }
  return false;
}
function isEcolesActivated(ec) {
  if (!ec) return false;
  if (ec.ips_min != null || ec.ips_max != null) {
    return true;
  }
  return false;
}

// ---------------------------------------------------------------------
// 3) Helpers : getCommunesFromDepartements (inchangé)
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
    const val = [dep];

    console.time(`getCommunesFromDep-${dep}`);
    let result = await pool.query(query, val);
    console.timeEnd(`getCommunesFromDep-${dep}`);

    let communesDep = result.rows.map(r => r.commune);
    allCommunes.push(...communesDep);
  }
  return Array.from(new Set(allCommunes));
}

// ---------------------------------------------------------------------
// 4) getCarresLocalisationAndInsecurite
// ---------------------------------------------------------------------
async function getCarresLocalisationAndInsecurite(params, criteria) {
  const { code_type, codes } = params;
  let communesSelection = [];

  // A) Obtenir la liste de communes
  console.time('A) localiser communes');
  if (code_type === 'com') {
    communesSelection = codes;
  } else if (code_type === 'dep') {
    let allCom = [];
    for (let dep of codes) {
      let comDep = await getCommunesFromDepartements([dep]);
      allCom = [...allCom, ...comDep];
    }
    communesSelection = Array.from(new Set(allCom));
  } else {
    throw new Error('code_type doit être "com" ou "dep".');
  }
  console.log('=> communesSelection.length =', communesSelection.length);
  console.timeEnd('A) localiser communes');

  if (!communesSelection.length) {
    return [];
  }

  // B) insécurité (intersection)
  if (criteria && criteria.insecurite && criteria.insecurite.min != null) {
    console.time('B) insecurite query');
    const queryIns = `
      SELECT insee_com
      FROM delinquance.notes_insecurite_geom_complet
      WHERE note_sur_20 >= $1
        AND insee_com = ANY($2)
    `;
    const valIns = [criteria.insecurite.min, communesSelection];
    let resIns = await pool.query(queryIns, valIns);
    console.timeEnd('B) insecurite query');

    let communesInsecOk = resIns.rows.map(r => r.insee_com);
    console.log('=> communesInsecOk.length =', communesInsecOk.length);

    console.time('B) intersection communes insecurite');
    communesSelection = intersectArrays(communesSelection, communesInsecOk);
    console.timeEnd('B) intersection communes insecurite');
    console.log('=> communesFinal (after insecurite) =', communesSelection.length);

    if (!communesSelection.length) {
      return [];
    }
  }

  // C) grille200m => arrayCarreLoc
  console.time('C) grille200m query');
  const queryCarrLoc = `
    SELECT idinspire AS id_carre_200m
    FROM decoupages.grille200m_metropole
    WHERE insee_com && $1
  `;
  const valCarrLoc = [communesSelection];
  let resCarrLoc = await pool.query(queryCarrLoc, valCarrLoc);
  console.timeEnd('C) grille200m query');

  let arrayCarreLoc = resCarrLoc.rows.map(r => r.id_carre_200m);
  console.log('=> arrayCarreLoc.length =', arrayCarreLoc.length);

  return arrayCarreLoc;
}

// ---------------------------------------------------------------------
// 5) Intersection stricte : DVF, Filosofi
// ---------------------------------------------------------------------
async function applyDVF(arrayCarreLoc, dvfCriteria) {
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

  if (dvfCriteria.propertyTypes && dvfCriteria.propertyTypes.length > 0) {
    whereClauses.push(`codtyploc = ANY($${idx})`);
    values.push(dvfCriteria.propertyTypes);
    idx++;
  }

  if (dvfCriteria.budget) {
    if (dvfCriteria.budget.min != null) {
      whereClauses.push(`valeurfonc >= $${idx}`);
      values.push(dvfCriteria.budget.min);
      idx++;
    }
    if (dvfCriteria.budget.max != null) {
      whereClauses.push(`valeurfonc <= $${idx}`);
      values.push(dvfCriteria.budget.max);
      idx++;
    }
  }

  if (dvfCriteria.surface) {
    if (dvfCriteria.surface.min != null) {
      whereClauses.push(`sbati >= $${idx}`);
      values.push(dvfCriteria.surface.min);
      idx++;
    }
    if (dvfCriteria.surface.max != null) {
      whereClauses.push(`sbati <= $${idx}`);
      values.push(dvfCriteria.surface.max);
      idx++;
    }
  }

  if (dvfCriteria.rooms) {
    if (dvfCriteria.rooms.min != null) {
      whereClauses.push(`nbpprinc >= $${idx}`);
      values.push(dvfCriteria.rooms.min);
      idx++;
    }
    if (dvfCriteria.rooms.max != null) {
      whereClauses.push(`nbpprinc <= $${idx}`);
      values.push(dvfCriteria.rooms.max);
      idx++;
    }
  }

  if (dvfCriteria.years) {
    if (dvfCriteria.years.min != null) {
      whereClauses.push(`anneemut >= $${idx}`);
      values.push(dvfCriteria.years.min);
      idx++;
    }
    if (dvfCriteria.years.max != null) {
      whereClauses.push(`anneemut <= $${idx}`);
      values.push(dvfCriteria.years.max);
      idx++;
    }
  }
  console.timeEnd('D) DVF build query');

  const whDVF = 'WHERE ' + whereClauses.join(' AND ');
  const queryDVF = `
    SELECT DISTINCT id_carre_200m
    FROM dvf_filtre.dvf_simplifie
    ${whDVF}
  `;

  console.time('D) DVF exec query');
  let resDVF = await pool.query(queryDVF, values);
  console.timeEnd('D) DVF exec query');

  let arrDVF = resDVF.rows.map(r => r.id_carre_200m);
  console.log('=> DVF rowCount =', arrDVF.length);

  console.time('D) DVF intersection');
  let result = intersectArrays(arrayCarreLoc, arrDVF);
  console.timeEnd('D) DVF intersection');
  console.log('=> after DVF intersectionSet.length =', result.length);

  return result;
}

async function applyFilosofi(arrayCarreLoc, filo) {
  if (!isFilosofiActivated(filo)) {
    return arrayCarreLoc;
  }
  console.time('E) Filosofi');
  let whereFilo = [];
  let valFilo = [];
  let iF = 1;

  whereFilo.push(`idcar_200m = ANY($${iF})`);
  valFilo.push(arrayCarreLoc);
  iF++;

  if (filo.nv_moyen) {
    if (filo.nv_moyen.min != null) {
      whereFilo.push(`nv_moyen >= $${iF}`);
      valFilo.push(filo.nv_moyen.min);
      iF++;
    }
    if (filo.nv_moyen.max != null) {
      whereFilo.push(`nv_moyen <= $${iF}`);
      valFilo.push(filo.nv_moyen.max);
      iF++;
    }
  }
  if (filo.part_log_soc) {
    if (filo.part_log_soc.min != null) {
      whereFilo.push(`part_log_soc >= $${iF}`);
      valFilo.push(filo.part_log_soc.min);
      iF++;
    }
    if (filo.part_log_soc.max != null) {
      whereFilo.push(`part_log_soc <= $${iF}`);
      valFilo.push(filo.part_log_soc.max);
      iF++;
    }
  }

  if (whereFilo.length <= 1) {
    console.timeEnd('E) Filosofi');
    // => rien de paramétré => on ne filtre pas
    return arrayCarreLoc;
  }

  const whFi = 'WHERE ' + whereFilo.join(' AND ');
  const queryFi = `
    SELECT idcar_200m
    FROM filosofi.c200_france_2019
    ${whFi}
  `;
  let resFi = await pool.query(queryFi, valFilo);
  let arrFi = resFi.rows.map(r => r.idcar_200m);
  console.log('=> Filosofi rowCount =', arrFi.length);

  console.time('E) Filosofi intersection');
  let result = intersectArrays(arrayCarreLoc, arrFi);
  console.timeEnd('E) Filosofi intersection');
  console.log('=> after Filosofi intersectionSet.length =', result.length);

  console.timeEnd('E) Filosofi');
  return result;
}

// ---------------------------------------------------------------------
// 6) Critères partiels : Ecoles, Collèges
// ---------------------------------------------------------------------

async function applyEcolesPartial(arrayCarreLoc, ecoles) {
  if (!isEcolesActivated(ecoles)) {
    return arrayCarreLoc;
  }
  console.time('G) Ecoles');
  // 1) subsetCouvert = carreaux sur Paris (insee_com ILIKE '751%')
  console.time('G) Ecoles subsetCouvert query');
  const queryParis = `
    SELECT idinspire
    FROM decoupages.grille200m_metropole
    WHERE idinspire = ANY($1)
      AND EXISTS (
        SELECT 1
        FROM unnest(insee_com) AS c
        WHERE c ILIKE '751%'
      )
  `;
  let resParis = await pool.query(queryParis, [arrayCarreLoc]);
  console.timeEnd('G) Ecoles subsetCouvert query');

  let subsetCouvert = resParis.rows.map(r => r.idinspire);
  console.log(`G) Ecoles => subsetCouvert.length = ${subsetCouvert.length}`);

  // 2) Filtrer subsetCouvert via la table pivot ecoles
  let wE = [];
  let vE = [];
  let iE = 1;

  wE.push(`id_carre_200m = ANY($${iE})`);
  vE.push(subsetCouvert);
  iE++;

  if (ecoles.ips_min != null) {
    wE.push(`ips >= $${iE}`);
    vE.push(ecoles.ips_min);
    iE++;
  }
  if (ecoles.ips_max != null) {
    wE.push(`ips <= $${iE}`);
    vE.push(ecoles.ips_max);
    iE++;
  }

  if (wE.length <= 1) {
    console.timeEnd('G) Ecoles');
    // => aucun param => on ne filtre pas subsetCouvert
    return arrayCarreLoc;
  }

  const queryEco = `
    SELECT DISTINCT id_carre_200m
    FROM education_ecoles.idcar200m_rne_ipsecoles
    WHERE ${wE.join(' AND ')}
  `;
  console.time('G) Ecoles pivot query');
  let resEco = await pool.query(queryEco, vE);
  console.timeEnd('G) Ecoles pivot query');

  let subsetCouvertFiltre = resEco.rows.map(r => r.id_carre_200m);
  console.log(`G) Ecoles => subsetCouvertFiltre.length = ${subsetCouvertFiltre.length}`);

  // 3) subsetHors = difference
  let subsetHors = differenceArrays(arrayCarreLoc, subsetCouvert);
  console.log(`G) Ecoles => subsetHors.length = ${subsetHors.length}`);

  // 4) union
  let result = unionArrays(subsetCouvertFiltre, subsetHors);
  console.log(`G) Ecoles => result.length = ${result.length}`);

  console.timeEnd('G) Ecoles');
  return result;
}

async function applyCollegesPartial(arrayCarreLoc, col) {
  if (!isCollegesActivated(col)) {
    return arrayCarreLoc;
  }
  console.time('F) Colleges');

  // Ex: départements manquants
  const DEPS_MANQUANTS = ['17','22','2A','29','2B','52','56']; 
  // 1) subsetCouvert => exclure dep manquants
  console.time('F) Colleges subsetCouvert query');
  const inClause = DEPS_MANQUANTS.map(d => `'${d}'`).join(',');
  const qCouv = `
    SELECT idinspire
    FROM decoupages.grille200m_metropole
    WHERE idinspire = ANY($1)
      AND NOT EXISTS (
        SELECT 1
        FROM unnest(insee_dep) d
        WHERE d IN ('17','22','2A','29','2B','52','56')
      )
  `;
  let resCouv = await pool.query(qCouv, [arrayCarreLoc]);
  console.timeEnd('F) Colleges subsetCouvert query');

  let subsetCouvert = resCouv.rows.map(r => r.idinspire);
  console.log(`F) Colleges => subsetCouvert.length = ${subsetCouvert.length}`);

  // 2) Filtrer via pivot
  let wCols = [];
  let valsCols = [];
  let iC = 1;

  wCols.push(`id_carre_200m = ANY($${iC})`);
  valsCols.push(subsetCouvert);
  iC++;

  if (col.valeur_figaro_min != null) {
    wCols.push(`niveau_college_figaro >= $${iC}`);
    valsCols.push(col.valeur_figaro_min);
    iC++;
  }
  if (col.valeur_figaro_max != null) {
    wCols.push(`niveau_college_figaro <= $${iC}`);
    valsCols.push(col.valeur_figaro_max);
    iC++;
  }

  if (wCols.length <= 1) {
    // => aucun param => pas de filtrage
    console.timeEnd('F) Colleges');
    return arrayCarreLoc;
  }

  console.time('F) Colleges pivot query');
  const qCols = `
    SELECT DISTINCT id_carre_200m
    FROM education_colleges.idcar200m_rne_niveaucolleges
    WHERE ${wCols.join(' AND ')}
  `;
  let resCols = await pool.query(qCols, valsCols);
  console.timeEnd('F) Colleges pivot query');

  let subsetCouvertFiltre = resCols.rows.map(r => r.id_carre_200m);
  console.log(`F) Colleges => subsetCouvertFiltre.length = ${subsetCouvertFiltre.length}`);

  // 3) subsetHors
  let subsetHors = differenceArrays(arrayCarreLoc, subsetCouvert);
  console.log(`F) Colleges => subsetHors.length = ${subsetHors.length}`);

  // 4) union
  let result = unionArrays(subsetCouvertFiltre, subsetHors);
  console.log(`F) Colleges => result.length = ${result.length}`);

  console.timeEnd('F) Colleges');
  return result;
}

// ---------------------------------------------------------------------
// 7) ROUTE POST /get_carreaux_filtre
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

    // 1) Localisation + insécurité
    let arrayCarreLoc = await getCarresLocalisationAndInsecurite(params, criteria);
    if (!arrayCarreLoc.length) {
      console.timeEnd('TOTAL /get_carreaux_filtre');
      return res.json({ nb_carreaux: 0, carreaux: [] });
    }

    // 2) DVF intersection stricte
    if (isDVFActivated(criteria?.dvf)) {
      arrayCarreLoc = await applyDVF(arrayCarreLoc, criteria.dvf);
      if (!arrayCarreLoc.length) {
        console.timeEnd('TOTAL /get_carreaux_filtre');
        return res.json({ nb_carreaux: 0, carreaux: [] });
      }
    }

    // 3) Filosofi intersection stricte
    if (isFilosofiActivated(criteria?.filosofi)) {
      arrayCarreLoc = await applyFilosofi(arrayCarreLoc, criteria.filosofi);
      if (!arrayCarreLoc.length) {
        console.timeEnd('TOTAL /get_carreaux_filtre');
        return res.json({ nb_carreaux: 0, carreaux: [] });
      }
    }

    // 4) Collèges => filtrage partiel
    if (isCollegesActivated(criteria?.colleges)) {
      arrayCarreLoc = await applyCollegesPartial(arrayCarreLoc, criteria.colleges);
      // pas de check si 0 => hors zone couverte est conservé
    }

    // 5) Ecoles => filtrage partiel
    if (isEcolesActivated(criteria?.ecoles)) {
      arrayCarreLoc = await applyEcolesPartial(arrayCarreLoc, criteria.ecoles);
      // idem
    }

    const intersectionSet = arrayCarreLoc;
    console.log('=> final intersectionSet.length =', intersectionSet.length);
    if (!intersectionSet.length) {
      console.timeEnd('TOTAL /get_carreaux_filtre');
      return res.json({ nb_carreaux: 0, carreaux: [] });
    }

    // (I) Communes regroupement
    // => On veut renvoyer un tableau "communes: [...]" 
    //    { insee_com, nom_com, insee_dep, nom_dep, nb_carreaux }
    console.time('I) Communes regroupement');
    // on n'a pas forcément la liste des communes sélectionnées (communesFinal),
    // tu peux faire un unnest, puis JOINTURE sur decoupages.communes
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
      GROUP BY e.insee, c.nom, c.insee_dep, c.nom_dep
      ORDER BY nb_carreaux DESC
    `;

    const communesRes = await pool.query(queryCommunes, [intersectionSet]);
    console.timeEnd('I) Communes regroupement');
    console.log('=> Nombre de communes distinctes =', communesRes.rowCount);

    let communesData = communesRes.rows.map(row => ({
      insee_com: row.insee_com,
      nom_com: row.nom_com,
      insee_dep: row.insee_dep,
      nom_dep: row.nom_dep,
      nb_carreaux: Number(row.nb_carreaux)
    }));

    // On construit la réponse finale
    const finalResponse = {
      nb_carreaux: intersectionSet.length,
      carreaux: intersectionSet,
      communes: communesData
    };

    console.timeEnd('TOTAL /get_carreaux_filtre');
    return res.json(finalResponse);

  } catch (error) {
    console.error('Erreur dans /get_carreaux_filtre :', error);
    console.timeEnd('TOTAL /get_carreaux_filtre');
    return res.status(500).json({ error: error.message });
  }
});

// ----------------------------------------------------
// Lancement serveur
// ----------------------------------------------------
const PORT = process.env.PORT || 3000;
app.listen(PORT, () => {
  console.log(`API démarrée sur le port ${PORT}`);
});
