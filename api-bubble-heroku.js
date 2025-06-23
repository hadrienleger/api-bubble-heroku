/****************************************************
 * Fichier : api-bubble-heroku_v3_iris.js
 *  - ajoute GET /iris_by_point?lat=...&lon=...
 *  - facteur commun buildIrisDetail()
 ****************************************************/
const express = require('express');
const cors = require('cors');
const { Pool } = require('pg');
const rateLimit = require('express-rate-limit');

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
app.set('trust proxy', 1);    // Express utilise X-Forwarded-For (Heroku)

// ----------------------------------
// Configuration CORS
// ----------------------------------
app.use(cors({
  origin: '*',
  methods: ['GET', 'POST', 'OPTIONS'],
  optionsSuccessStatus: 200
}));

app.use(express.json());

// Anti-scraping — même limite pour tous les endpoints
const limiter = rateLimit({
  windowMs: 15 * 60 * 1000,   // 15 min
  max: 1000,
  standardHeaders: true,
  legacyHeaders: false,
  handler: (req, res) => {
    console.log(`Rate limit atteint pour IP: ${req.ip}`);
    res.status(429).json({ error: 'Too Many Requests' });
  }
});
app.use(limiter);

// ------------------------------------------------------------------
// >>>>>>  UTILITAIRES et FONCTIONS DE FILTRES  <<<<<<
// ------------------------------------------------------------------

// --------------------------------------------------------------
// A) Fonctions utilitaires (intersection, union, différence)
// --------------------------------------------------------------
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

  if (Array.isArray(dvf.propertyTypes)) {
    dvf.propertyTypes = dvf.propertyTypes.filter(pt => pt != null);
  }

  const hasType = dvf.propertyTypes && dvf.propertyTypes.length > 0;
  const hasBudget = dvf.budget && (
    (dvf.budget.min != null) || (dvf.budget.max != null)
  );
  const hasSurface = dvf.surface && (
    (dvf.surface.min != null) || (dvf.surface.max != null)
  );
  const hasRooms = dvf.rooms && (
    (dvf.rooms.min != null) || (dvf.rooms.max != null)
  );
  const hasYears = dvf.years && (
    (dvf.years.min != null) || (dvf.years.max != null)
  );

  return (hasType || hasBudget || hasSurface || hasRooms || hasYears);
}

function isRevenusActivated(rev) {
  if (!rev) return false;
  if (rev.mediane_rev_decl && (rev.mediane_rev_decl.min != null || rev.mediane_rev_decl.max != null)) return true;
  return false;
}
function isLogSocActivated(ls) {
  if (!ls) return false;
  if (ls.part_log_soc && (ls.part_log_soc.min != null || ls.part_log_soc.max != null)) return true;
  return false;
}
function isCollegesActivated(col) {
  if (!col) return false;
  if (col.valeur_figaro_min != null || col.valeur_figaro_max != null) return true;
  return false;
}
function isEcolesActivated(ec) {
  if (!ec) return false;
  if (ec.ips_min != null || ec.ips_max != null) return true;
  return false;
}

// --------------------------------------------------------------
// C) Récupérer communes à partir de départements
// --------------------------------------------------------------
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

async function getArrondissementsForVilleGlobale(codeVille) {
  const sql = `
    SELECT DISTINCT insee_arm
    FROM decoupages.communes
    WHERE insee_com = $1
      AND insee_arm IS NOT NULL
      AND insee_arm <> ''
  `;
  let r = await pool.query(sql, [codeVille]);
  return r.rows.map(row => row.insee_arm);
}

async function gatherCommuneCodes(selectedLocalities) {
  let allCodes = [];

  for (let loc of selectedLocalities) {
    if (loc.type_collectivite === "Département") {
      console.time(`getCommunesFromDep-${loc.code_insee}`);
      let result = await getCommunesFromDepartements([loc.code_insee]);
      console.timeEnd(`getCommunesFromDep-${loc.code_insee}`);
      allCodes.push(...result);
    } else {
      if (loc.type_collectivite === "commune"
        && ["75056", "69123", "13055"].includes(loc.code_insee)
      ) {
        let arrCodes = await getArrondissementsForVilleGlobale(loc.code_insee);
        allCodes.push(...arrCodes);
      } else {
        allCodes.push(loc.code_insee);
      }
    }
  }

  return Array.from(new Set(allCodes));
}

// --------------------------------------------------------------
// D) getIrisLocalisationAndSecurite
// --------------------------------------------------------------
async function getIrisLocalisationAndSecurite(params) {
  console.time('A) localiser communes');

  if (!params.selected_localities || !Array.isArray(params.selected_localities)) {
    throw new Error('Paramètre "selected_localities" manquant ou invalide (doit être un array).');
  }

  let communesSelection = await gatherCommuneCodes(params.selected_localities);
  console.log('=> communesSelection.length =', communesSelection.length);
  console.timeEnd('A) localiser communes');

  if (!communesSelection.length) {
    return { arrayIrisLoc: [], communesFinal: [] };
  }

  let communesFinal = communesSelection;

  console.time('C) iris_grandeetendue_2022 query');
  const qIris = `
    SELECT code_iris
    FROM decoupages.iris_grandeetendue_2022
    WHERE insee_com = ANY($1)
  `;
  let rIris = await pool.query(qIris, [communesFinal]);
  console.timeEnd('C) iris_grandeetendue_2022 query');

  let arrayIrisLoc = rIris.rows.map(rr => rr.code_iris);
  console.log('=> arrayIrisLoc.length =', arrayIrisLoc.length);

  return { arrayIrisLoc, communesFinal };
}

// --------------------------------------------------------------
// D) Filtrage DVF
// --------------------------------------------------------------
async function getDVFCountTotal(irisList) {
  if (!irisList.length) {
    return {};
  }

  console.time('getDVFCountTotal');
  const sql = `
    SELECT code_iris, COUNT(*)::int AS nb_total
    FROM dvf_filtre.dvf_simplifie
    WHERE code_iris = ANY($1)
    GROUP BY code_iris
  `;
  let res = await pool.query(sql, [irisList]);
  console.timeEnd('getDVFCountTotal');

  let dvfTotalByIris = {};
  for (let row of res.rows) {
    dvfTotalByIris[row.code_iris] = Number(row.nb_total);
  }
  return dvfTotalByIris;
}

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
// D) Filtrage DVF bis => prix du mètre carré médian
// --------------------------------------------------------------
async function applyPrixMedian(irisList, pmCriteria) {
  if (!irisList.length) {
    return { irisSet: [], prixMedianByIris: {} };
  }

  let whereClauses = [
    `code_iris = ANY($1)`,
    `periode_prix = '2024-S1'`
  ];
  let vals = [irisList];
  let idx = 2;

  let doIntersection = false;

  if (pmCriteria?.min != null) {
    whereClauses.push(`prix_median >= $${idx}`);
    vals.push(pmCriteria.min);
    idx++;
    doIntersection = true;
  }

  if (pmCriteria?.max != null) {
    whereClauses.push(`prix_median <= $${idx}`);
    vals.push(pmCriteria.max);
    idx++;
    doIntersection = true;
  }

  let sql = `
    SELECT code_iris, prix_median
    FROM dvf_filtre.prix_m2_iris
    WHERE ${whereClauses.join(' AND ')}
  `;
  let result = await pool.query(sql, vals);

  let prixMedianByIris = {};
  let irisOK = [];
  for (let row of result.rows) {
    prixMedianByIris[row.code_iris] = Number(row.prix_median);
    irisOK.push(row.code_iris);
  }

  let irisSet = doIntersection
    ? irisList.filter(ci => irisOK.includes(ci))
    : irisList;

  return { irisSet, prixMedianByIris };
}

// --------------------------------------------------------------
// E) Filtrage revenus déclarés
// --------------------------------------------------------------
async function applyRevenus(irisList, revCriteria) {
  console.time('E) Revenus: build query');
  
  if (!irisList.length) {
    return { irisSet: [], revenusByIris: {} };
  }

  let whereClauses = [];
  let vals = [];
  let idx = 1;

  whereClauses.push(`code_iris = ANY($${idx})`);
  vals.push(irisList);
  idx++;

  let doIntersection = false;
  if (revCriteria && revCriteria.mediane_rev_decl) {
    if (revCriteria.mediane_rev_decl.min != null) {
      whereClauses.push(`mediane_rev_decl >= $${idx}`);
      vals.push(revCriteria.mediane_rev_decl.min);
      idx++;
      doIntersection = true;
    }
    if (revCriteria.mediane_rev_decl.max != null) {
      whereClauses.push(`mediane_rev_decl <= $${idx}`);
      vals.push(revCriteria.mediane_rev_decl.max);
      idx++;
      doIntersection = true;
    }
  }

  let query = `
    SELECT code_iris, mediane_rev_decl
    FROM filosofi.rev_decl_hl_2021
    WHERE ${whereClauses.join(' AND ')}
  `;
  console.timeEnd('E) Revenus: build query');

  console.time('E) Revenus: exec');
  let r = await pool.query(query, vals);
  console.timeEnd('E) Revenus: exec');
  
  let revenusByIris = {};
  let irisOK = [];
  for (let row of r.rows) {
    let ci = row.code_iris;
    let mv = row.mediane_rev_decl != null ? Number(row.mediane_rev_decl) : null;
    revenusByIris[ci] = { mediane_rev_decl: mv };
    irisOK.push(ci);
  }

  let irisSet;
  if (doIntersection) {
    irisSet = intersectArrays(irisList, irisOK);
  } else {
    irisSet = irisList;
  }

  return { irisSet, revenusByIris };
}

// --------------------------------------------------------------
// F) Filtrage Logements sociaux
// --------------------------------------------------------------
async function applyLogSoc(irisList, lsCriteria) {
  if (!irisList.length) return { irisSet: [], logSocByIris: {} };

  let whereClauses = [ `code_iris = ANY($1)` ];
  let vals = [ irisList ];
  let idx = 2;
  let doIntersection = false;

  if (lsCriteria && lsCriteria.part_log_soc) {
    if (lsCriteria.part_log_soc.min != null) {
      whereClauses.push(`part_log_soc >= $${idx}`);
      vals.push(lsCriteria.part_log_soc.min);
      idx++;
      doIntersection = true;
    }
    if (lsCriteria.part_log_soc.max != null) {
      whereClauses.push(`part_log_soc <= $${idx}`);
      vals.push(lsCriteria.part_log_soc.max);
      idx++;
      doIntersection = true;
    }
  }

  let query = `
    SELECT code_iris, part_log_soc
    FROM filosofi.logements_sociaux_iris_hl_2021
    WHERE ${whereClauses.join(' AND ')}
  `;

  let r = await pool.query(query, vals);

  let logSocByIris = {};
  let irisOK = [];
  for (let row of r.rows) {
    logSocByIris[row.code_iris] = { part_log_soc: Number(row.part_log_soc) };
    irisOK.push(row.code_iris);
  }

  let irisSet = doIntersection
    ? intersectArrays(irisList, irisOK)
    : irisList;

  return { irisSet, logSocByIris };
}

// --------------------------------------------------------------
// G) Filtrage Sécurité (mode rayon)
// -------------------------------------------------------------
// --------------------------------------------------------------
// G) Filtrage Sécurité (mode rayon) - VERSION DEBUG
// -------------------------------------------------------------
async function applySecurite(irisList, secCrit) {
  console.log('DEBUG applySecurite - INPUT:', {
    irisListLength: irisList?.length || 0,
    secCrit: secCrit
  });

  if (!irisList.length || !secCrit) {
    console.log('DEBUG applySecurite - Pas de critères ou liste vide, retour sans filtrage');
    return { irisSet: irisList, securiteByIris: {} };
  }

  const { min, max } = secCrit;
  if (min == null && max == null) {
    console.log('DEBUG applySecurite - Min et max null, retour sans filtrage');
    return { irisSet: irisList, securiteByIris: {} };
  }

  let where = ['i.code_iris = ANY($1)'];
  let vals  = [irisList];
  let idx   = 2;

  if (min != null) {
    where.push(`d.note_sur_20 >= $${idx}`);
    vals.push(min);
    idx++;
    console.log('DEBUG applySecurite - Ajout critère min:', min);
  }
  if (max != null) {
    where.push(`d.note_sur_20 <= $${idx}`);
    vals.push(max);
    console.log('DEBUG applySecurite - Ajout critère max:', max);
  }

  const sql = `
    SELECT i.code_iris, d.note_sur_20
    FROM decoupages.iris_grandeetendue_2022 i
    JOIN decoupages.communes c
         ON (c.insee_com = i.insee_com OR c.insee_arm = i.insee_com)
    JOIN delinquance.notes_insecurite_geom_complet d
         ON (d.insee_com = c.insee_com OR d.insee_com = c.insee_arm)
    WHERE ${where.join(' AND ')}
  `;
  
  console.log('DEBUG applySecurite - SQL:', sql);
  console.log('DEBUG applySecurite - Params:', vals);
  
  const r = await pool.query(sql, vals);
  
  console.log('DEBUG applySecurite - Résultats query:', {
    rowCount: r.rowCount,
    firstRows: r.rows.slice(0, 3) // Affiche les 3 premiers résultats
  });

  const securiteByIris = {};
  const irisOK = [];
  for (const row of r.rows) {
    securiteByIris[row.code_iris] = [{ note: Number(row.note_sur_20) }];
    irisOK.push(row.code_iris);
  }

  const finalIrisSet = intersectArrays(irisList, irisOK);
  
  console.log('DEBUG applySecurite - OUTPUT:', {
    inputLength: irisList.length,
    foundLength: irisOK.length,
    finalLength: finalIrisSet.length,
    sampleFinalIris: finalIrisSet.slice(0, 5) // 5 premiers IRIS finaux
  });

  return {
    irisSet: finalIrisSet,
    securiteByIris
  };
}


// --------------------------------------------------------------
// H) Critère partiel Ecoles
// --------------------------------------------------------------
async function applyEcoles(irisList, ecolesCrit) {
  console.time('applyEcoles');

  if (!irisList.length) {
    console.timeEnd('applyEcoles');
    return {
      irisSet: [],
      ecolesByIris: {}
    };
  }

  console.time('Ecoles coverage');
  const qCoverage = `
    SELECT DISTINCT code_iris
    FROM education_ecoles.iris_rne_ipsecoles
    WHERE code_iris = ANY($1)
  `;
  let coverageRes = await pool.query(qCoverage, [irisList]);
  console.timeEnd('Ecoles coverage');

  let coverageSet = new Set(coverageRes.rows.map(r => r.code_iris));
  let subsetCouvert = irisList.filter(ci => coverageSet.has(ci));
  let subsetHors = irisList.filter(ci => !coverageSet.has(ci));

  let ecolesByIris = {};
  for (let ci of subsetHors) {
    ecolesByIris[ci] = "hors-scope";
  }

  if (!subsetCouvert.length) {
    console.timeEnd('applyEcoles');
    return {
      irisSet: subsetHors,
      ecolesByIris
    };
  }

  console.time('Ecoles pivot');
  let wPivot = [`code_iris = ANY($1)`];
  let vPivot = [subsetCouvert];
  let idx = 2;

  let doIntersection = false;
  if (ecolesCrit && ecolesCrit.ips_min != null) {
    wPivot.push(`ips >= $${idx}`);
    vPivot.push(ecolesCrit.ips_min);
    idx++;
    doIntersection = true;
  }
  if (ecolesCrit && ecolesCrit.ips_max != null) {
    wPivot.push(`ips <= $${idx}`);
    vPivot.push(ecolesCrit.ips_max);
    idx++;
    doIntersection = true;
  }

  const sqlPivot = `
    SELECT code_iris, code_rne, ips, nom_ecole
    FROM education_ecoles.iris_rne_ipsecoles
    WHERE ${wPivot.join(' AND ')}
  `;
  let pivotRes = await pool.query(sqlPivot, vPivot);
  console.timeEnd('Ecoles pivot');

  let irisFoundSet = new Set();
  let mapEcoles = {};
  for (let row of pivotRes.rows) {
    let ci = row.code_iris;
    irisFoundSet.add(ci);
    if (!mapEcoles[ci]) mapEcoles[ci] = [];
    mapEcoles[ci].push({
      code_rne: row.code_rne,
      ips: Number(row.ips),
      nom_ecole: row.nom_ecole
    });
  }

  let finalSet;
  if (doIntersection) {
    finalSet = subsetCouvert.filter(ci => irisFoundSet.has(ci));
  } else {
    finalSet = subsetCouvert;
  }

  for (let ci of finalSet) {
    ecolesByIris[ci] = mapEcoles[ci] || [];
  }

  let irisFinal = finalSet.concat(subsetHors);

  console.log(`applyEcoles => coverageRes=${coverageRes.rowCount} pivotRes=${pivotRes.rowCount}`);
  console.timeEnd('applyEcoles');

  return {
    irisSet: irisFinal,
    ecolesByIris
  };
}

// --------------------------------------------------------------
// I) Critère partiel Collèges
// --------------------------------------------------------------
async function applyColleges(irisList, colCrit) {
  console.time('applyColleges');

  if (!irisList.length) {
    console.timeEnd('applyColleges');
    return {
      irisSet: [],
      collegesByIris: {}
    };
  }

  const DEPS_MANQUANTS = ['17', '22', '2A', '29', '2B', '52', '56'];
  console.time('Colleges coverage');
  const sqlCov = `
    SELECT code_iris, insee_dep
    FROM decoupages.iris_grandeetendue_2022
    WHERE code_iris = ANY($1)
  `;
  let covRes = await pool.query(sqlCov, [irisList]);
  console.timeEnd('Colleges coverage');

  let coverageSet = new Set();
  for (let row of covRes.rows) {
    if (!DEPS_MANQUANTS.includes(row.insee_dep)) {
      coverageSet.add(row.code_iris);
    }
  }

  let subsetCouvert = irisList.filter(ci => coverageSet.has(ci));
  let subsetHors = irisList.filter(ci => !coverageSet.has(ci));

  let collegesByIris = {};
  for (let ci of subsetHors) {
    collegesByIris[ci] = "hors-scope";
  }

  if (!subsetCouvert.length) {
    console.timeEnd('applyColleges');
    return {
      irisSet: subsetHors,
      collegesByIris
    };
  }

  console.time('Colleges pivot');
  let wPivot = [`code_iris = ANY($1)`];
  let vals = [subsetCouvert];
  let idx = 2;

  let doIntersection = false;
  if (colCrit && colCrit.valeur_figaro_min != null) {
    wPivot.push(`note_figaro_sur_20 >= $${idx}`);
    vals.push(colCrit.valeur_figaro_min);
    idx++;
    doIntersection = true;
  }
  if (colCrit && colCrit.valeur_figaro_max != null) {
    wPivot.push(`note_figaro_sur_20 <= $${idx}`);
    vals.push(colCrit.valeur_figaro_max);
    idx++;
    doIntersection = true;
  }

  const sqlPivot = `
    SELECT code_iris, code_rne,
           nom_college,
           note_figaro_sur_20
    FROM education_colleges.iris_rne_niveaux_2024
    WHERE ${wPivot.join(' AND ')}
  `;
  let pivotRes = await pool.query(sqlPivot, vals);
  console.timeEnd('Colleges pivot');

  let irisFoundSet = new Set();
  let mapCols = {};
  for (let row of pivotRes.rows) {
    let ci = row.code_iris;
    irisFoundSet.add(ci);
    if (!mapCols[ci]) mapCols[ci] = [];
    mapCols[ci].push({
      code_rne: row.code_rne,
      nom_college: row.nom_college,
      note_sur_20: Number(row.note_figaro_sur_20)
    });
  }

  let finalSet;
  if (doIntersection) {
    finalSet = subsetCouvert.filter(ci => irisFoundSet.has(ci));
  } else {
    finalSet = subsetCouvert;
  }

  for (let ci of finalSet) {
    collegesByIris[ci] = mapCols[ci] || [];
  }

  let irisFinal = finalSet.concat(subsetHors);

  console.log(`applyColleges => coverageRes=${covRes.rowCount} pivotRes=${pivotRes.rowCount}`);
  console.timeEnd('applyColleges');

  return {
    irisSet: irisFinal,
    collegesByIris
  };
}

// --------------------------------------------------------------
// J) gatherSecuByIris
// --------------------------------------------------------------
async function gatherSecuriteByIris(irisList) {
  if (!irisList.length) {
    return { securiteByIris: {}, irisNameByIris: {} };
  }

  console.time('Securite details: query');
  const q = `
    SELECT i.code_iris,
           i.nom_iris,
           d.note_sur_20
    FROM decoupages.iris_grandeetendue_2022 i
    LEFT JOIN decoupages.communes c
           ON (c.insee_com = i.insee_com OR c.insee_arm = i.insee_com)
    LEFT JOIN delinquance.notes_insecurite_geom_complet d
           ON (d.insee_com = c.insee_com OR d.insee_com = c.insee_arm)
    WHERE i.code_iris = ANY($1)
  `;
  let r = await pool.query(q, [irisList]);
  console.timeEnd('Securite details: query');

  let securiteByIris = {};
  let irisNameByIris = {};
  for (let row of r.rows) {
    let noteValue = (row.note_sur_20 != null) ? Number(row.note_sur_20) : null;
    securiteByIris[row.code_iris] = [{ note: noteValue }];
    irisNameByIris[row.code_iris] = row.nom_iris || '(iris inconnu)';
  }

  return { securiteByIris, irisNameByIris };
}

// --------------------------------------------------------------
// K) groupByCommunes
// --------------------------------------------------------------
async function groupByCommunes(irisList, communesFinal) {
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
      JOIN decoupages.iris_grandeetendue_2022 i ON i.code_iris = s.iris
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

// ------------------------------------------------------------------
// FONCTION COMMUNE : construit la fiche quartier
// ------------------------------------------------------------------
// ------------------------------------------------------------------
// FONCTION COMMUNE : construit la fiche quartier complète
// ------------------------------------------------------------------
async function buildIrisDetail(irisCodes) {
  /* 1️⃣  Premier passage : on vérifie le flux DVF.
        applyDVF renvoie :
          - afterDVF : la liste des IRIS qu’il trouve        (array)
          - dvfCountByIris : nombre de mutations filtrées    (objet)
        Note : si aucun critère DVF n’est activé, afterDVF == irisCodes.
  */
  const { irisSet: afterDVF, dvfCountByIris } =
    await applyDVF(irisCodes, null);

  /* 2️⃣  Toutes les fonctions suivantes doivent travailler
        EXACTEMENT sur ce même ensemble afterDVF, sinon on risque
        d’avoir des tableaux vides ou incohérents.              */
  const dvfTotalByIris               = await getDVFCountTotal(afterDVF);
  const { revenusByIris }            = await applyRevenus(afterDVF,  null);
  const { logSocByIris }             = await applyLogSoc(afterDVF,   null);
  const { prixMedianByIris }         = await applyPrixMedian(afterDVF, null);
  const { ecolesByIris }             = await applyEcoles(afterDVF,   null);
  const { collegesByIris }           = await applyColleges(afterDVF, null);
  const { securiteByIris,
          irisNameByIris }           = await gatherSecuriteByIris(afterDVF);

  //  Passage qui va nous servir à envoyer la commune au endpoint iris_by_point
  const sqlCom = `
    SELECT i.code_iris,
           -- Paris, Lyon, Marseille : on prend insee_arm si non vide
           COALESCE(NULLIF(c.insee_arm, ''), c.insee_com) AS insee_com,
           c.nom AS nom_com
    FROM decoupages.iris_grandeetendue_2022 i
    JOIN decoupages.communes c
         ON (c.insee_com = i.insee_com OR c.insee_arm = i.insee_com)
    WHERE i.code_iris = ANY($1)
  `;
  const comRes = await pool.query(sqlCom, [afterDVF]);

  // ⇒ { "751176510": { insee_com:"75117", nom_com:"Paris 17e Arrondissement" } }
  const communeByIris = {};
  for (const row of comRes.rows) {
    communeByIris[row.code_iris] = {
      insee_com : row.insee_com,
      nom_com   : row.nom_com
    };
  }


  /* 3️⃣  On assemble l’objet final en bouclant sur afterDVF,
        pas sur irisCodes. */
  const irisFinalDetail = [];

  for (const iris of afterDVF) {
    const commune = communeByIris[iris] ?? {};
    irisFinalDetail.push({
      code_iris        : iris,
      nom_iris         : irisNameByIris[iris]       ?? null,
      insee_com        : commune.insee_com         ?? null,   // 🆕
      nom_commune      : commune.nom_com           ?? null,   // 🆕
      dvf_count        : dvfCountByIris[iris]       ?? 0,
      dvf_count_total  : dvfTotalByIris[iris]       ?? 0,
      mediane_rev_decl : revenusByIris[iris]?.mediane_rev_decl ?? null,
      part_log_soc     : logSocByIris[iris]?.part_log_soc     ?? null,
      securite         : securiteByIris[iris]?.[0]?.note      ?? null,
      ecoles           : ecolesByIris[iris]                   ?? [],
      colleges         : collegesByIris[iris]                 ?? [],
      prix_median_m2   : prixMedianByIris[iris]               ?? null
    });
  }

  return irisFinalDetail;   // ← tableau d’un ou plusieurs objets
}


// ------------------------------------------------------------------
// POST /get_iris_filtre  (localisation + critères éventuels)
// ------------------------------------------------------------------
app.post('/get_iris_filtre', async (req, res) => {
  console.log('>>> BODY RECEIVED FROM BUBBLE:', JSON.stringify(req.body, null, 2));
  console.time('TOTAL /get_iris_filtre');

  try {
    /************  0.  LOCALISATION GÉNÉRIQUE  ****************/
    const { mode, codes_insee, center, radius_km, criteria = {} } = req.body;

    /************  0.bis  PARAMÉTRAGE RECHERCHE SANS LOCALISATION (CRITERIA-ONLY)  ****************/
    /* -----------------------------------------------------------
     *  RACCROCHAGE DIRECT : si Bubble envoie déjà une liste IRIS
     * --------------------------------------------------------- */
    if (Array.isArray(req.body.iris_base) && req.body.iris_base.length) {
      console.log(`🔄 Bypass localisation : ${req.body.iris_base.length} IRIS reçus`);

      // A) on ré-utilise directement cette liste
      let arrayIrisLoc  = req.body.iris_base;

      // B) petites communes associées (pour l’onglet “Communes”)
      const sql = `
        SELECT DISTINCT insee_com
        FROM decoupages.iris_grandeetendue_2022
        WHERE code_iris = ANY($1)
      `;
      const { rows }   = await pool.query(sql, [arrayIrisLoc]);
      let  communesFinal = rows.map(r => r.insee_com);

      /* --- et on saute directement au bloc de filtres --- */
await _applyAllFiltersAndRespond(res, arrayIrisLoc, communesFinal, criteria, 'rayon');
      return;   // plus besoin du code juste après
    }

    let arrayIrisLoc  = [];   // liste des codes IRIS trouvés
    let communesFinal = [];   // communes concernées (pour l’onglet « Communes »)

    /* ---------- MODE 1 : collectivités ---------- */
    if (mode === 'collectivites') {
      const fakeParams = {
        selected_localities: (codes_insee || []).map(c => ({
          code_insee:        c,
          type_collectivite: 'commune'    // valeur neutre, juste pour ré-utiliser la fonction
        }))
      };

      const r = await getIrisLocalisationAndSecurite(fakeParams);

      arrayIrisLoc  = r.arrayIrisLoc;
      communesFinal = r.communesFinal;

    /* ---------- MODE 2 : cercle rayon ---------- */
    } else if (mode === 'rayon') {
      const { lon, lat } = center;
      const radius_m = Number(radius_km) * 1000;   // 0.5 km → 500 m
      const sql = `
       SELECT code_iris
       FROM decoupages.iris_grandeetendue_2022
       WHERE ST_DWithin(
               geom_2154,
               ST_Transform(
                 ST_SetSRID(ST_MakePoint($1,$2),4326), 2154),
               $3                       -- distance en mètres
             )
      `;
      const { rows } = await pool.query(sql, [lon, lat, radius_m]);
      arrayIrisLoc = rows.map(r => r.code_iris);

      /* communesFinal = toutes les communes touchées */
      const qCom = `
        SELECT DISTINCT insee_com
        FROM decoupages.iris_grandeetendue_2022
        WHERE code_iris = ANY($1)
      `;
      const cRes = await pool.query(qCom, [arrayIrisLoc]);
      communesFinal = cRes.rows.map(r => r.insee_com);

    /* ---------- mode inconnu ---------- */
    } else {
      return res.status(400).json({ error: 'mode invalid' });
    }

    /* Aucun IRIS trouvé → réponse vide */
    if (!arrayIrisLoc.length) {
      console.timeEnd('TOTAL /get_iris_filtre');
      return res.json({ nb_iris: 0, iris: [], communes: [] });
    }

console.log('📬 _applyAllFiltersAndRespond() CALLED');
    /* ✅ On a trouvé des IRIS : on lance maintenant tous les filtres */
await _applyAllFiltersAndRespond(res, arrayIrisLoc, communesFinal, criteria, mode);
    return;      // on sort, le reste du handler ne s’exécute plus

  } catch (err) {
    console.error('Erreur dans /get_iris_filtre :', err);
    console.timeEnd('TOTAL /get_iris_filtre');
    return res.status(500).json({ error: err.message });
  }
});

async function _applyAllFiltersAndRespond(res, arrayIrisLoc, communesFinal, criteria, mode = null) {
  if (!arrayIrisLoc.length) {
    console.timeEnd('TOTAL /get_iris_filtre');
    return res.json({ nb_iris: 0, iris: [], communes: [] });
  }

  let iris = arrayIrisLoc;
  let securiteFromApply = {};
  let dvfCountByIris = {};
  let revenusByIris = {};
  let prixMedianByIris = {};
  let ecolesByIris = {};
  let collegesByIris = {};

  // — Sécurité —
  console.log('🔍 Application du filtre sécurité');
  const resSecu = await applySecurite(iris, criteria?.securite);
  iris = resSecu.irisSet;
  securiteFromApply = resSecu.securiteByIris;

  if (!iris.length) {
    console.timeEnd('TOTAL /get_iris_filtre');
    return res.json({ nb_iris: 0, iris: [], communes: [] });
  }

  // — Prix médian m2 —
  console.log('🔍 Application du filtre prix median');
  const resPrix = await applyPrixMedian(iris, criteria?.prixMedianM2);
  iris = resPrix.irisSet;
  prixMedianByIris = resPrix.prixMedianByIris;

  if (!iris.length) {
    console.timeEnd('TOTAL /get_iris_filtre');
    return res.json({ nb_iris: 0, iris: [], communes: [] });
  }

  // — DVF —
  console.log('🔍 Application du filtre DVF');
  const resDVF = await applyDVF(iris, criteria?.dvf);
  iris = resDVF.irisSet;
  dvfCountByIris = resDVF.dvfCountByIris;

  if (!iris.length) {
    console.timeEnd('TOTAL /get_iris_filtre');
    return res.json({ nb_iris: 0, iris: [], communes: [] });
  }

  // — FILOSOFI —
  console.log('🔍 Application du filtre revenus');
  const resRev = await applyRevenus(iris, criteria?.filosofi);
  iris = resRev.irisSet;
  revenusByIris = resRev.revenusByIris;

  if (!iris.length) {
    console.timeEnd('TOTAL /get_iris_filtre');
    return res.json({ nb_iris: 0, iris: [], communes: [] });
  }

  // — ÉCOLES —
  console.log('🔍 Application du filtre écoles');
  const resEco = await applyEcoles(iris, criteria?.ecoles);
  iris = resEco.irisSet;
  ecolesByIris = resEco.ecolesByIris;

  if (!iris.length) {
    console.timeEnd('TOTAL /get_iris_filtre');
    return res.json({ nb_iris: 0, iris: [], communes: [] });
  }

  // — COLLÈGES —
  console.log('🔍 Application du filtre collèges');
  const resCol = await applyColleges(iris, criteria?.colleges);
  iris = resCol.irisSet;
  collegesByIris = resCol.collegesByIris;

  if (!iris.length) {
    console.timeEnd('TOTAL /get_iris_filtre');
    return res.json({ nb_iris: 0, iris: [], communes: [] });
  }

  // ✅ Final response
  console.log('✅ Tous les filtres appliqués →', iris.length, 'IRIS');

  return res.json({
    nb_iris: iris.length,
    iris,
    communes: communesFinal,
    securite: securiteFromApply,
    prixMedianM2: prixMedianByIris,
    dvf: dvfCountByIris,
    revenus: revenusByIris,
    ecoles: ecolesByIris,
    colleges: collegesByIris
  });
}


// ------------------------------------------------------------------
// NOUVEAU ENDPOINT : GET /iris_by_point?lat=...&lon=...
// ------------------------------------------------------------------
/* --------------------------------------------------------------
   GET /iris_by_point
   --------------------------------------------------------------
   Params :
      lat, lon        (obligatoires)
      radius_km       (optionnel, défaut 0.3)
   Retour :
      {
        nb_iris : n,
        iris    : [ { … + est_cible:true/false } ],
        communes: [ … ]
      }
---------------------------------------------------------------- */
app.get('/iris_by_point', async (req, res) => {
  const { lat, lon, radius_km = '0.3' } = req.query;
  if (!lat || !lon) {
    return res.status(400).json({ error: 'lat & lon are required' });
  }

  try {
    const radius_m = Number(radius_km) * 1000;     // ⇒ mètres

    /* A) cible = l’IRIS qui contient le point ----------------- */
    const cibleSql = `
      SELECT code_iris, geom_2154
      FROM decoupages.iris_grandeetendue_2022
      WHERE ST_Contains(
              geom_2154,
              ST_Transform(ST_SetSRID(ST_MakePoint($1,$2),4326),2154)
            )
      LIMIT 1
    `;
    const cibleRes = await pool.query(cibleSql, [lon, lat]);
    if (!cibleRes.rows.length) {
      return res.status(404).json({ error: 'IRIS not found' });
    }
    const codeCible = cibleRes.rows[0].code_iris;

    /* B) voisins = IRIS coupant le disque ---------------------- */
    const voisinsSql = `
      SELECT i.code_iris
      FROM decoupages.iris_grandeetendue_2022 i
      WHERE ST_DWithin(
              i.geom_2154,
              ST_Transform(ST_SetSRID(ST_MakePoint($1,$2),4326),2154),
              $3
            )
    `;
    const vRes = await pool.query(voisinsSql, [lon, lat, radius_m]);

    const irisList = vRes.rows.map(r => r.code_iris);
    if (!irisList.includes(codeCible)) irisList.push(codeCible); // sécurité

    /* C) détail complet via buildIrisDetail -------------------- */
    const detail = await buildIrisDetail(irisList);   // déjà existant

    /* D) flag est_cible et communes --------------------------- */
    const enriched = detail.map(r => ({
      ...r,
      est_cible: r.code_iris === codeCible
    }));

    const communesData = await groupByCommunes(
      irisList,
      enriched.map(r => r.insee_com).filter(Boolean)
    );

    res.json({
      nb_iris : enriched.length,
      iris    : enriched,
      communes: communesData
    });

  } catch (err) {
    console.error('/iris_by_point error:', err);
    res.status(500).json({ error: 'server' });
  }
});


// ------------------------------------------------------------------
// PING
// ------------------------------------------------------------------
app.get('/ping', async (_req, res) => {
  try {
    await pool.query('SELECT 1');
    res.json({ message: 'pong', db_status: 'ok', date: new Date() });
  } catch (e) {
    res.status(500).json({ message: 'pong', db_status: 'error', error: e.message });
  }
});

// ------------------------------------------------------------------
// CENTROID (NOUVEAU ENDPOINT) - ABANDONNE MAIS JE LE GARDE AU CAS OÙ
// ------------------------------------------------------------------
// ------------------------------------------------------------------
//  POST /centroids
//  Corps attendu : tableau JSON
//    [
//      { "code_insee":"75056", "type_collectivite":"commune"      },
//      { "code_insee":"75106", "type_collectivite":"arrondissement"},
//      { "code_insee":"44",    "type_collectivite":"Département"   }
//    ]
//  Réponse : [{ code_insee, lon, lat, type }, …]
// ------------------------------------------------------------------
app.post('/centroids', async (req, res) => {
  const input = req.body;
  if (!Array.isArray(input)) {
    return res.status(400).json({ error: 'Liste attendue (array JSON)' });
  }

  const arrondissements  = input.filter(x => x.type_collectivite === 'arrondissement')
                                .map(x => x.code_insee);

  const communesGlobales = input.filter(x => x.type_collectivite === 'commune')
                                .map(x => x.code_insee);

  const departements     = input.filter(x => x.type_collectivite === 'Département')
                                .map(x => x.code_insee);

  try {
    const results = [];

    /* ---------- 1. Arrondissements ---------- */
    if (arrondissements.length) {
      const sqlArr = `
        SELECT
          insee_arm AS code_insee,
          ST_X(ST_Transform(ST_PointOnSurface(geom_2154),4326)) AS lon,
          ST_Y(ST_Transform(ST_PointOnSurface(geom_2154),4326)) AS lat
        FROM decoupages.communes
        WHERE insee_arm = ANY($1)
      `;
      const { rows } = await pool.query(sqlArr, [arrondissements]);
      results.push(...rows.map(r => ({
        code_insee: r.code_insee,
        lon:        r.lon,
        lat:        r.lat,
        type:       'arrondissement'
      })));
    }

    /* ---------- 2. Communes globales ---------- */
    if (communesGlobales.length) {
      const sqlCom = `
        WITH unions AS (
          SELECT
            insee_com,
            ST_Union(geom_2154) AS geom_union   -- union même s'il n'y a qu'un polygone
          FROM decoupages.communes
          WHERE insee_com = ANY($1)
          GROUP BY insee_com
        )
        SELECT
          insee_com AS code_insee,
          ST_X(ST_Transform(ST_PointOnSurface(geom_union),4326)) AS lon,
          ST_Y(ST_Transform(ST_PointOnSurface(geom_union),4326)) AS lat
        FROM unions
      `;
      const { rows } = await pool.query(sqlCom, [communesGlobales]);
      results.push(...rows.map(r => ({
        code_insee: r.code_insee,
        lon:        r.lon,
        lat:        r.lat,
        type:       'commune'
      })));
    }

    /* ---------- 3. Départements ---------- */
    if (departements.length) {
      const sqlDep = `
        SELECT
          insee_dep AS code_insee,
          ST_X(ST_Transform(ST_PointOnSurface(geom_2154),4326)) AS lon,
          ST_Y(ST_Transform(ST_PointOnSurface(geom_2154),4326)) AS lat
        FROM decoupages.departements
        WHERE insee_dep = ANY($1)
      `;
      const { rows } = await pool.query(sqlDep, [departements]);
      results.push(...rows.map(r => ({
        code_insee: r.code_insee,
        lon:        r.lon,
        lat:        r.lat,
        type:       'Département'
      })));
    }

    res.set('Cache-Control', 'public, max-age=3600');
    return res.json(results);            // [{ code_insee, lon, lat, type }, …]
  } catch (err) {
    console.error(err);
    return res.status(500).json({ error: 'server_error' });
  }
});

// ------------------------------------------------------------------
// COLLECTIVITES (NOUVEAU ENDPOINT)
// ------------------------------------------------------------------
// ------------------------------------------------------------------
// ------------------------------------------------------------------
//  POST /collectivites_polygons
//  Corps attendu : [{code_insee, type_collectivite}, …]
//  Réponse       : FeatureCollection GeoJSON
// ------------------------------------------------------------------
app.post('/collectivites_polygons', async (req, res) => {
  const input = req.body;
  if (!Array.isArray(input)) return res.status(400).json({error:'array required'});

  const arr  = input.filter(x => x.type_collectivite === 'arrondissement')
                    .map(x => x.code_insee);
  const com  = input.filter(x => x.type_collectivite === 'commune')
                    .map(x => x.code_insee);
  const dep  = input.filter(x => x.type_collectivite === 'Département')
                    .map(x => x.code_insee);

  const features = [];

  /* -- 1. arrondissements -------------------------------- */
  if (arr.length){
    const sql = `
      SELECT insee_arm AS code,
             ST_AsGeoJSON(ST_Transform(geom_2154,4326)) AS geo
      FROM decoupages.communes
      WHERE insee_arm = ANY($1)
    `;
    const {rows} = await pool.query(sql,[arr]);
    rows.forEach(r => features.push({
      type:'Feature',
      geometry: JSON.parse(r.geo),
      properties:{ code_insee:r.code, type:'arrondissement' }
    }));
  }

  /* -- 2. communes globales (union si P,L,M) -------------- */
  if (com.length){
    const sql = `
      WITH un AS (
        SELECT insee_com,
               ST_Union(geom_2154) AS geom
        FROM decoupages.communes
        WHERE insee_com = ANY($1)
        GROUP BY insee_com
      )
      SELECT insee_com AS code,
             ST_AsGeoJSON(ST_Transform(geom,4326)) AS geo
      FROM un;
    `;
    const {rows} = await pool.query(sql,[com]);
    rows.forEach(r => features.push({
      type:'Feature',
      geometry: JSON.parse(r.geo),
      properties:{ code_insee:r.code, type:'commune' }
    }));
  }

  /* -- 3. départements ------------------------------------ */
  if (dep.length){
    const sql = `
      SELECT insee_dep AS code,
             ST_AsGeoJSON(ST_Transform(geom_2154,4326)) AS geo
      FROM decoupages.departements
      WHERE insee_dep = ANY($1)
    `;
    const {rows} = await pool.query(sql,[dep]);
    rows.forEach(r => features.push({
      type:'Feature',
      geometry: JSON.parse(r.geo),
      properties:{ code_insee:r.code, type:'Département' }
    }));
  }

  res.set('Cache-Control','public,max-age=3600');
  res.json({ type:'FeatureCollection', features });
});




// ------------------------------------------------------------------
// LANCEMENT
// ------------------------------------------------------------------
const PORT = process.env.PORT || 3000;
app.listen(PORT, () => {
  console.log(`API IRIS v3 démarrée sur le port ${PORT}`);
});