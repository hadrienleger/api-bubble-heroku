/****************************************************
 * Fichier : api-bubble-heroku_v3_iris.js
 *  - facteur commun buildIrisDetail()
 ****************************************************/
const express = require('express');
const cors = require('cors');
const { Pool } = require('pg');
const rateLimit = require('express-rate-limit');

/**  Préfixes d’équipements gérés  ------------------------------- */
const EQUIP_PREFIXES = [
  'boulang',   // boulangerie-pâtisserie
  'bouche',    // commerces de bouche
  'superm',    // super/hypermarchés
  'epicerie',  // épiceries / supérettes
  'lib',       // librairies
  'cinema',    // cinémas
  'conserv',   // conservatoires
  'magbio'     // magasins bio
];

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
  if (col.niveau_coll_min != null || col.niveau_coll_max != null) return true;
  return false;
}
function isEcolesActivated(ec) {
  if (!ec) return false;
  return (
    (ec.ips_min != null || ec.ips_max != null) ||   // filtrage IPS
    ec.rayon != null                                // OU simple rayon
  );
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

/**
 * Devine si un code correspond à un Département ou une Commune.
 * - Départements : 1–95, 971–974, 976, 2A, 2B…
 * - Communes    : 5 caractères numériques (75056, 35238…)
 */
function looksLikeDepartement(code){
  const s = String(code);
  // Codes numériques 1–95 ou 971–976
  if (/^\d{1,3}$/.test(s))     return true;   // 01, 93, 976…
  // Codes Corse 2A / 2B
  if (/^\d{2}[AB]$/.test(s))   return true;   // 2A, 2B
  return false;
}

async function gatherCommuneCodes(selectedLocalities) {
  let allCodes = [];

  for (let loc of selectedLocalities) {
    /* -----------------------------------------------------------------
       ① Correction automatique : si on reçoit « commune » mais que le
         code ressemble clairement à un département, on corrige.
    ------------------------------------------------------------------*/
    if (loc.type_collectivite === 'commune' && looksLikeDepartement(loc.code_insee)) {
      loc.type_collectivite = 'Département';
    }

    if (loc.type_collectivite === "Département") {
      console.time(`getCommunesFromDep-${loc.code_insee}`);
      let result = await getCommunesFromDepartements([loc.code_insee]);
      console.timeEnd(`getCommunesFromDep-${loc.code_insee}`);
      allCodes.push(...result);
    } else {
      if (loc.type_collectivite === "commune" && ["75056", "69123", "13055"].includes(loc.code_insee)) {
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
    FROM filosofi.logsoc_iris_hl_2021
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
// G) Filtrage Sécurité (mode rayon) - VERSION DEBUG
// -------------------------------------------------------------
async function applySecurite(irisList, secCrit) {
  if (!irisList.length) {
    return { irisSet: irisList, securiteByIris: {} };
  }

  const { min, max } = secCrit || {};
  const hasFilter = min != null || max != null;

  // Toujours récupérer TOUTES les notes
  const sql = `
    SELECT code_iris, note_sur_20
    FROM delinquance.iris_securite_2023
    WHERE code_iris = ANY($1)
  `;
  const { rows } = await pool.query(sql, [irisList]);

  const securiteByIris = {};
  const irisWithValidNotes = [];
  
  for (const r of rows) {
    const note = r.note_sur_20 !== null ? Number(r.note_sur_20) : null;
    securiteByIris[r.code_iris] = [{ note }];
    
    // Pour le filtrage, on ne garde que ceux qui respectent les bornes
    if (!hasFilter || 
        (note !== null && 
         (min == null || note >= min) && 
         (max == null || note <= max))) {
      irisWithValidNotes.push(r.code_iris);
    }
  }

  // Si pas de filtre actif, on retourne tous les IRIS
  // Si filtre actif, on ne retourne que ceux qui respectent les critères
  const irisSet = hasFilter 
    ? irisList.filter(ci => irisWithValidNotes.includes(ci))
    : irisList;

  return { irisSet, securiteByIris };
}

// --------------------------------------------------------------
// H) Critère partiel Écoles (toujours 300 m par défaut,
//     filtrage IPS/rayon/secteur seulement si l’utilisateur l’active)
// --------------------------------------------------------------
async function applyEcolesRadius(irisList, ec) {
  /* 1.  Valeurs par défaut + détection du filtrage explicite */
  ec = ec || {};

  const rayon   = ec.rayon   ?? 300;          // 300 m si rien n'est précisé
  const ips_min = ec.ips_min ?? null;
  const ips_max = ec.ips_max ?? null;

  // Nettoyer le tableau de secteurs en enlevant les null/undefined
  let secteursArr;
  if (Array.isArray(ec.secteurs)) {
    const cleaned = ec.secteurs.filter(s => s != null && s !== '');
    secteursArr = cleaned.length > 0 ? cleaned : ['PU','PR'];
  } else if (ec.secteur) {
    secteursArr = [ec.secteur];
  } else {
    secteursArr = ['PU','PR'];
  }

  // Détection plus robuste du filtrage actif
  const filteringActive =
    (ips_min !== null || 
     ips_max !== null ||
     (ec.rayon != null && ec.rayon !== 300) ||  // Filtrage seulement si différent de la valeur par défaut
     (Array.isArray(ec.secteurs) && ec.secteurs.filter(s => s != null).length > 0) ||
     ec.secteur != null);

  /* 2.  Construction de la requête */
  let p = 1;
  const vals  = [irisList, rayon, secteursArr];
  const where = [
    `code_iris = ANY($${p++})`,
    `rayon     = $${p++}`,
    `secteur   = ANY($${p++})`
  ];
  if (ips_min !== null) { where.push(`ips >= $${p}`); vals.push(ips_min); p++; }
  if (ips_max !== null) { where.push(`ips <= $${p}`); vals.push(ips_max); p++; }

  const sql = `
    SELECT p.code_iris,
           p.code_rne,
           p.ips,
           p.distance_m,
           g.patronyme_uai,
           g.secteur_public_prive_libe,
           g.adresse_uai,
           g.code_postal_uai,
           g.libelle_commune        AS commune_nom
    FROM   education_ecoles.iris_ecoles_ips_rayon_2025 AS p
    LEFT JOIN   education.geoloc_etab_2025                 AS g
           ON g.numero_uai = p.code_rne
    WHERE  ${where.join(' AND ')}
  `;

  /* 3. Exécution */
  const { rows } = await pool.query(sql, vals);

  /* 4. Agrégation en mémoire */
  const irisOK       = new Set();
  const ecolesByIris = {};

  for (const r of rows) {
    irisOK.add(r.code_iris);
    if (!ecolesByIris[r.code_iris]) ecolesByIris[r.code_iris] = [];
    ecolesByIris[r.code_iris].push({
      code_rne   : r.code_rne,
      ips        : Number(r.ips),
      distance_m : r.distance_m,
      nom        : r.patronyme_uai,
      secteur    : r.secteur_public_prive_libe,
      adresse    : r.adresse_uai,
      cp         : r.code_postal_uai,
      commune    : r.commune_nom
    });
  }

  /* 5. Jeu d'IRIS final */
  let irisSet;
  if (filteringActive) {
    // Si filtrage actif → intersection
    irisSet = Array.from(irisOK);
  } else {
    // Si pas de filtrage → on garde tous les IRIS d'entrée
    irisSet = irisList;
    
    // S'assurer que ecolesByIris contient bien les données pour tous les IRIS
    for (const iris of irisList) {
      if (!ecolesByIris[iris]) {
        ecolesByIris[iris] = [];
      }
    }
  }

  return { irisSet, ecolesByIris };
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
  if (colCrit && colCrit.niveau_coll_min != null) {
    wPivot.push(`note_figaro_sur_20 >= $${idx}`);
    vals.push(colCrit.niveau_coll_min);
    idx++;
    doIntersection = true;
  }
  if (colCrit && colCrit.niveau_coll_max != null) {
    wPivot.push(`note_figaro_sur_20 <= $${idx}`);
    vals.push(colCrit.niveau_coll_max);
    idx++;
    doIntersection = true;
  }

const sqlPivot = `
  SELECT p.code_iris,
         p.code_rne,
         g.patronyme_uai,
         p.note_figaro_sur_20,
         g.adresse_uai,
         g.code_postal_uai,
         g.libelle_commune AS commune_nom
  FROM   education_colleges.iris_rne_niveaux_2024 AS p
  LEFT JOIN education.geoloc_etab_2025            AS g
         ON g.numero_uai = p.code_rne
  WHERE  ${wPivot.join(' AND ')}
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
      nom_college: row.patronyme_uai,
      note_sur_20: Number(row.note_figaro_sur_20),
      adresse    : row.adresse_uai,
      cp         : row.code_postal_uai,
      commune    : row.commune_nom
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
// J) Filtrage des crèches
// --------------------------------------------------------------
function isCrechesActivated(cr) {
  if (!cr) return false;
  return cr.min != null || cr.max != null;
}

async function applyCreches(irisList, crechesCrit) {
  if (!irisList.length) return { irisSet: [], crechesByIris: {} };

  // 0. Valeurs par défaut quand l’utilisateur n’a fixé aucune borne
  const { min = null, max = null } = crechesCrit || {};

  const sql = `
    SELECT i.code_iris,
           cr.txcouv_eaje_com
    FROM decoupages.iris_grandeetendue_2022 i
    LEFT JOIN decoupages.communes c
           ON (c.insee_com = i.insee_com OR c.insee_arm = i.insee_com)
LEFT JOIN education_creches.tauxcouverture_communes_2022 cr
       ON (cr.numcom = c.insee_com OR cr.numcom = c.insee_arm)
       AND cr.annee = 2022          -- condition déplacée dans le ON pour garder le left-join
WHERE i.code_iris = ANY($1)
      AND ($2::numeric IS NULL OR cr.txcouv_eaje_com IS NULL OR cr.txcouv_eaje_com >= $2)
      AND ($3::numeric IS NULL OR cr.txcouv_eaje_com IS NULL OR cr.txcouv_eaje_com <= $3)
  `;

  const { rows } = await pool.query(sql, [irisList, min, max]);

  const crechesByIris = {};
  const irisOK = [];

  for (const r of rows) {
    crechesByIris[r.code_iris] = r.txcouv_eaje_com != null ? Number(r.txcouv_eaje_com) : null;
    irisOK.push(r.code_iris);
  }

  return {
    irisSet: intersectArrays(irisList, irisOK),
    crechesByIris,
  };
}

// --------------------------------------------------------------
// K) Filtrage des équipements (par score composite en fonction de la localisation dans et à proximité du quartier)
// --------------------------------------------------------------
/**
 * Filtre la liste d’IRIS sur la base d’un score d’équipement.
 *   - prefix  : 'boulang', 'bouche', … (doit exister dans EQUIP_PREFIXES)
 *   - criteria: {min: <num>|null, max: <num>|null}
 */
async function applyScoreEquip(irisList, prefix, criteria = {}) {
  if (!irisList.length || !EQUIP_PREFIXES.includes(prefix)) {
    return { irisSet: irisList, scoreByIris: {} };
  }

  const { min = null, max = null } = criteria;
  const col = `${prefix}_score`;                       // ex. boulang_score

  const sql = `
    SELECT code_iris, ${col} AS score
    FROM   equipements.iris_equip_2024
    WHERE  code_iris = ANY($1)
      AND ($2::numeric IS NULL OR ${col} >= $2)
      AND ($3::numeric IS NULL OR ${col} <= $3)
  `;

  const { rows } = await pool.query(sql, [irisList, min, max]);

  const scoreByIris = {};
  const keep = new Set();
  for (const r of rows) {
    scoreByIris[r.code_iris] = Number(r.score);
    keep.add(r.code_iris);
  }

  /*  – si aucune borne n’a été fixée → pas d’intersection
      – sinon on conserve uniquement les IRIS qui satisfont la requête   */
  const irisSet = (min == null && max == null)
        ? irisList
        : irisList.filter(ci => keep.has(ci));

  return { irisSet, scoreByIris };
}



// --------------------------------------------------------------
// K) gatherSecuByIris
// --------------------------------------------------------------
async function gatherSecuriteByIris(irisList) {
  if (!irisList.length) {
    return { securiteByIris: {}, irisNameByIris: {} };
  }

  console.time('Securite details: query');
  const q = `
    SELECT i.code_iris,
           i.nom_iris,
           s.note_sur_20
    FROM decoupages.iris_grandeetendue_2022 i
    LEFT JOIN delinquance.iris_securite_2023 s
           ON s.code_iris = i.code_iris
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
// L) groupByCommunes
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
// FONCTION COMMUNE : construit la fiche quartier complète
// ------------------------------------------------------------------
// ------------------------------------------------------------------
// FONCTION COMMUNE : construit la fiche quartier complète
// ------------------------------------------------------------------
async function buildIrisDetail(irisCodes, criteria = {}, equipCriteria = {}) {
  console.time('buildIrisDetail');
  try {
    /* 1️⃣  DVF --------------------------------------------------- */
    const dvfRes          = await applyDVF(irisCodes, criteria?.dvf);
    let   irisCurrent     = dvfRes.irisSet;
    const dvfCountByIris  = dvfRes.dvfCountByIris;
    const dvfTotalByIris  = await getDVFCountTotal(irisCurrent);

    /* 2️⃣  Revenus ---------------------------------------------- */
    const revRes          = await applyRevenus(irisCurrent,  criteria?.filosofi);
    irisCurrent           = revRes.irisSet;
    const revenusByIris   = revRes.revenusByIris;

    /* 3️⃣  Logements sociaux ------------------------------------ */
    const lsRes           = await applyLogSoc(irisCurrent,   criteria?.filosofi);
    irisCurrent           = lsRes.irisSet;
    const logSocByIris    = lsRes.logSocByIris;

    /* 4️⃣  Prix médian m² --------------------------------------- */
    const prixRes         = await applyPrixMedian(irisCurrent, criteria?.prixMedianM2);
    irisCurrent           = prixRes.irisSet;
    const prixMedianByIris= prixRes.prixMedianByIris;

    /* 5️⃣  Écoles (IPS + rayon + secteur) ----------------------- */
    const ecolesRes       = await applyEcolesRadius(irisCurrent, criteria?.ecoles);
    irisCurrent           = ecolesRes.irisSet;
    const ecolesByIris    = ecolesRes.ecolesByIris;

    /* 6️⃣  Collèges --------------------------------------------- */
    const colRes          = await applyColleges(irisCurrent,  criteria?.colleges);
    irisCurrent           = colRes.irisSet;
    const collegesByIris  = colRes.collegesByIris;

    /* 7️⃣  Crèches ---------------------------------------------- */
    const crechesRes      = await applyCreches(irisCurrent,   criteria?.creches);
    irisCurrent           = crechesRes.irisSet;
    const crechesByIris   = crechesRes.crechesByIris;

    /* 8️⃣  Équipements (scores) ---------------------------------------------- */
    let scoreEquipByIris = {};      // agrège tous les scores demandés

    for (const prefix of EQUIP_PREFIXES) {
      if (!equipCriteria[prefix]) continue;      // pas demandé par l’utilisateur

      const res = await applyScoreEquip(irisCurrent, prefix, equipCriteria[prefix]);
      irisCurrent           = res.irisSet;
      scoreEquipByIris[prefix] = res.scoreByIris;

      // Si plus aucun IRIS ne passe, inutile de poursuivre la boucle
      if (!irisCurrent.length) break;
    }


  /* 8️⃣  Sécurité  ------------ */
  const secRes          = await applySecurite(irisCurrent, criteria?.securite);
  irisCurrent           = secRes.irisSet;          
  const securiteByIris  = secRes.securiteByIris;   // ← contient déjà TOUTES les notes

  /* ➡️  Compléter avec les noms d'IRIS (PAS les notes, on les a déjà) */
  const { irisNameByIris } = await gatherSecuriteByIris(irisCurrent);

    /* 9️⃣  Commune & département -------------------------------- */
    const sqlCom = `
      SELECT i.code_iris,
             COALESCE(NULLIF(c.insee_arm, ''), c.insee_com) AS insee_com,
             c.nom       AS nom_com,
             c.insee_dep AS code_dep,
             d.nom       AS nom_dep
      FROM decoupages.iris_grandeetendue_2022 i
      JOIN decoupages.communes     c ON (c.insee_com = i.insee_com OR c.insee_arm = i.insee_com)
      JOIN decoupages.departements d ON c.insee_dep = d.insee_dep
      WHERE i.code_iris = ANY($1)
    `;
    const comRes = await pool.query(sqlCom, [irisCurrent]);

    const communeByIris = {};
    for (const row of comRes.rows) {
      communeByIris[row.code_iris] = {
        nom_commune: row.nom_com,
        code_dep   : row.code_dep,
        nom_dep    : row.nom_dep
      };
    }

    /* 9️⃣ bis  BBOX des IRIS */
    console.time('BBOX query');
const bboxSql = `
  WITH sel AS (SELECT unnest($1::text[]) AS code_iris)
  SELECT sel.code_iris,
         ST_XMin(g) AS west,
         ST_YMin(g) AS south,
         ST_XMax(g) AS east,
         ST_YMax(g) AS north
  FROM sel
  JOIN LATERAL (
    SELECT ST_Transform(geom_2154,4326) AS g
    FROM decoupages.iris_grandeetendue_2022
    WHERE code_iris = sel.code_iris
    LIMIT 1
  ) sub ON true
`;
const { rows: bboxRows } = await pool.query(bboxSql, [irisCurrent]);
console.timeEnd('BBOX query');

const bboxByIris = {};
for (const b of bboxRows) {
  bboxByIris[b.code_iris] = [Number(b.west), Number(b.south),
                             Number(b.east), Number(b.north)];
}


    /* 🔟  Assemblage de la réponse finale ----------------------- */
    const irisFinalDetail = irisCurrent.map(iris => {
      const commune = communeByIris[iris] ?? {};
      const bbox    = bboxByIris[iris]    ?? [null,null,null,null];

      return {
        code_iris        : iris,
        nom_iris         : irisNameByIris[iris]           ?? null,
        commune          : {
          nom_commune : commune.nom_commune ?? null,
          nom_dep     : commune.nom_dep     ?? null,
          code_dep    : commune.code_dep    ?? null
        },
        dvf_count        : dvfCountByIris[iris]           ?? 0,
        dvf_count_total  : dvfTotalByIris[iris]           ?? 0,
        mediane_rev_decl : revenusByIris[iris]?.mediane_rev_decl ?? null,
        part_log_soc     : logSocByIris[iris]?.part_log_soc     ?? null,
        securite         : securiteByIris[iris]?.[0]?.note      ?? null,
        ecoles           : ecolesByIris[iris]             ?? [],
        colleges         : collegesByIris[iris]           ?? [],
        prix_median_m2   : prixMedianByIris[iris]         ?? null,
        taux_creches     : crechesByIris[iris]            ?? null,
        score_boulang  : scoreEquipByIris['boulang']?.[iris] ?? null,
        score_bouche   : scoreEquipByIris['bouche']?.[iris]  ?? null,
        score_superm   : scoreEquipByIris['superm']?.[iris]  ?? null,
        score_epicerie : scoreEquipByIris['epicerie']?.[iris]?? null,
        score_lib      : scoreEquipByIris['lib']?.[iris]     ?? null,
        score_cinema   : scoreEquipByIris['cinema']?.[iris]  ?? null,
        score_conserv  : scoreEquipByIris['conserv']?.[iris] ?? null,
        score_magbio   : scoreEquipByIris['magbio']?.[iris]  ?? null,
            bbox_w : bbox[0],
    bbox_s : bbox[1],
    bbox_e : bbox[2],
    bbox_n : bbox[3]
      };
    });

    console.timeEnd('buildIrisDetail');
    return irisFinalDetail;

  } catch (err) {
    console.error('Error in buildIrisDetail:', err);
    console.timeEnd('buildIrisDetail');
    throw err;
  }
}

// ---------------------------
// Helpers pour get_iris_data
// ---------------------------

const RAYONS_ECOLES = [300, 600, 1000, 2000, 5000];
const RAYONS_COMM   = ['in_iris', '300', '600', '1000'];

/** BBox 4326 depuis la table "petiteetendue" (comme l'ancien /iris/:code/bbox) */
async function fetchIrisBbox4326(codeIris) {
  const sql = `
    SELECT
      ST_XMin(geom) AS west,
      ST_YMin(geom) AS south,
      ST_XMax(geom) AS east,
      ST_YMax(geom) AS north
    FROM decoupages.iris_petiteetendue_2022
    WHERE code_iris = $1
    LIMIT 1
  `;
  const { rows } = await pool.query(sql, [codeIris]);
  if (!rows.length) return null;
  const b = rows[0];
  return [Number(b.west), Number(b.south), Number(b.east), Number(b.north)];
}

/** Écoles pour tous les rayons (en 1 requête), sans effet “filtre” */
async function fetchEcolesAllRayons(codeIris) {
  const out = {};
  for (const r of RAYONS_ECOLES) out[String(r)] = [];

  const sql = `
    SELECT p.rayon,
           p.code_rne,
           p.ips,
           p.secteur,
           p.distance_m,
           g.patronyme_uai                 AS nom,
           g.secteur_public_prive_libe     AS secteur_lib,
           g.adresse_uai                   AS adresse,
           g.code_postal_uai               AS cp,
           g.libelle_commune               AS commune
    FROM education_ecoles.iris_ecoles_ips_rayon_2025 p
    JOIN education.geoloc_etab_2025 g
      ON g.numero_uai = p.code_rne
    WHERE p.code_iris = $1
      AND p.rayon     = ANY($2)
    ORDER BY p.rayon, p.distance_m ASC
  `;

  const { rows } = await pool.query(sql, [codeIris, RAYONS_ECOLES]);

  for (const r of rows) {
    const key = String(r.rayon);
    out[key].push({
      rne        : r.code_rne,
      nom        : r.nom,
      // on expose “secteur” tel qu’en base ; si tu préfères le libellé humain :
      secteur    : r.secteur,           // code court (PU/PR si c’est le cas)
      secteur_lib: r.secteur_lib,       // libellé public/privé
      type       : r.type,
      ips        : r.ips != null ? Number(r.ips) : null,
      distance_m : Number(r.distance_m),
      adresse    : r.adresse,
      cp         : r.cp,
      commune    : r.commune
    });
  }

  return out;
}

/** HLM détaillé (tous champs) */
async function fetchHlmDetail(codeIris) {
  const q = `
    SELECT nblspls, part_log_soc, txlsplai, txlsplus, txlspls, txlspli
    FROM filosofi.logsoc_iris_hl_2021
    WHERE code_iris = $1
  `;
  const { rows } = await pool.query(q, [codeIris]);
  if (!rows.length) return null;
  const r = rows[0];
  return {
    nblspls      : r.nblspls      != null ? Number(r.nblspls) : null,
    part_log_soc : r.part_log_soc != null ? Number(r.part_log_soc) / 100 : null,
    txlsplai     : r.txlsplai     != null ? Number(r.txlsplai) / 100     : null,
    txlsplus     : r.txlsplus     != null ? Number(r.txlsplus) / 100     : null,
    txlspls      : r.txlspls      != null ? Number(r.txlspls) / 100      : null,
    txlspli      : r.txlspli      != null ? Number(r.txlspli) / 100      : null
  };
}

/** Commerces : reproduit la même structure que /get_all_commerces */
async function fetchCommercesAll(codeIris) {
  // 1) Liste des préfixes/typequ à partir de equipements.parametres
  const prefixQuery = `
    SELECT equip_prefix, typequ_codes
    FROM equipements.parametres;
  `;
  const { rows: prefixes } = await pool.query(prefixQuery);
  const equipPrefixes = prefixes.map(p => ({ prefix: p.equip_prefix, codes: p.typequ_codes }));

  // 2) Initialiser la structure { prefix: { in_iris:{count,items}, 300:{...}, 600:{...}, 1000:{...} } }
  const commerces = {};
  for (const { prefix } of equipPrefixes) {
    commerces[prefix] = {
      in_iris: { count: 0, items: [] },
      300    : { count: 0, items: [] },
      600    : { count: 0, items: [] },
      1000   : { count: 0, items: [] }
    };
  }
  // S’assure que 'magbio' existe (au cas où pas dans parametres)
  if (!commerces.magbio) {
    commerces.magbio = {
      in_iris: { count: 0, items: [] },
      300    : { count: 0, items: [] },
      600    : { count: 0, items: [] },
      1000   : { count: 0, items: [] }
    };
  }

  // 3) Magasins bio (source dédiée)
  for (const rayon of RAYONS_COMM) {
    let listSql, countSql, params;
    if (rayon === 'in_iris') {
      listSql = `
        SELECT
          TRIM(COALESCE(raison_sociale, '') || ' (' || COALESCE(denomination, '') || ')') AS nom,
          TRIM(COALESCE(addr_lieu::text,'') || ' ' || COALESCE(addr_cp::text,'') || ' ' || COALESCE(addr_ville::text,'')) AS adresse
        FROM equipements.magasins_bio_0725
        WHERE code_iris = $1
          AND cert_etat = 'ENGAGEE'
          AND code_iris IS NOT NULL
        ORDER BY nom
        LIMIT 50;
      `;
      countSql = `
        SELECT COUNT(*) AS total
        FROM equipements.magasins_bio_0725
        WHERE code_iris = $1
          AND cert_etat = 'ENGAGEE'
          AND code_iris IS NOT NULL;
      `;
      params = [codeIris];
    } else {
      const dist = parseInt(rayon, 10);
      listSql = `
        WITH iris_check AS (
          SELECT code_iris, geom_2154
          FROM decoupages.iris_grandeetendue_2022
          WHERE code_iris = $1::text
          LIMIT 1
        )
        SELECT
          TRIM(COALESCE(m.raison_sociale,'') || ' (' || COALESCE(m.denomination,'') || ')') AS nom,
          TRIM(COALESCE(m.addr_lieu::text,'') || ' ' || COALESCE(m.addr_cp::text,'') || ' ' || COALESCE(m.addr_ville::text,'')) AS adresse
        FROM equipements.magasins_bio_0725 m
        CROSS JOIN iris_check i
        WHERE m.cert_etat = 'ENGAGEE'
          AND m.geom_2154 IS NOT NULL
          AND m.code_iris IS NOT NULL
          AND ST_DWithin(m.geom_2154, i.geom_2154, $2)
        ORDER BY nom
        LIMIT 50;
      `;
      countSql = `
        WITH iris_check AS (
          SELECT code_iris, geom_2154
          FROM decoupages.iris_grandeetendue_2022
          WHERE code_iris = $1::text
          LIMIT 1
        )
        SELECT COUNT(*) AS total
        FROM equipements.magasins_bio_0725 m
        CROSS JOIN iris_check i
        WHERE m.cert_etat = 'ENGAGEE'
          AND m.geom_2154 IS NOT NULL
          AND m.code_iris IS NOT NULL
          AND ST_DWithin(m.geom_2154, i.geom_2154, $2);
      `;
      params = [codeIris, dist];
    }
    const { rows: list }   = await pool.query(listSql, params);
    const { rows: counts } = await pool.query(countSql, params);
    commerces.magbio[rayon] = {
      count: parseInt(counts[0]?.total || 0, 10),
      items: list.map(r => ({ nom: r.nom, adresse: r.adresse }))
    };
  }

  // 4) Autres types (BPE 2024)
  const otherPrefixes = equipPrefixes.filter(p => p.prefix !== 'magbio');
  for (const { prefix, codes } of otherPrefixes) {
    for (const rayon of RAYONS_COMM) {
      let listSql, countSql, params;
      if (rayon === 'in_iris') {
        listSql = `
          SELECT
            TRIM(COALESCE(nomrs,'') || ' ' || COALESCE(cnomrs,'')) AS nom,
            TRIM(COALESCE(numvoie,'') || ' ' || COALESCE(indrep,'') || ' ' ||
                 COALESCE(typvoie,'') || ' ' || COALESCE(libvoie,'') || ' ' ||
                 COALESCE(cadr,'')    || ' ' || COALESCE(codpos,'')  || ' ' ||
                 COALESCE(libcom,'')
            ) AS adresse
          FROM equipements.base_2024
          WHERE code_iris = $1
            AND typequ    = ANY($2)
          ORDER BY nom
          LIMIT 50;
        `;
        countSql = `
          SELECT COUNT(*) AS total
          FROM equipements.base_2024
          WHERE code_iris = $1
            AND typequ    = ANY($2);
        `;
        params = [codeIris, codes];
      } else {
        const dist = parseInt(rayon, 10);
        listSql = `
          WITH iris AS (
            SELECT geom_2154
            FROM decoupages.iris_grandeetendue_2022
            WHERE code_iris = $1::text
          )
          SELECT
            TRIM(COALESCE(nomrs,'') || ' ' || COALESCE(cnomrs,'')) AS nom,
            TRIM(COALESCE(numvoie,'') || ' ' || COALESCE(indrep,'') || ' ' ||
                 COALESCE(typvoie,'') || ' ' || COALESCE(libvoie,'') || ' ' ||
                 COALESCE(cadr,'')    || ' ' || COALESCE(codpos,'')  || ' ' ||
                 COALESCE(libcom,'')
            ) AS adresse
          FROM equipements.base_2024 b, iris i
          WHERE b.typequ = ANY($2)
            AND ST_DWithin(b.geom_2154, i.geom_2154, $3)
          ORDER BY nom
          LIMIT 50;
        `;
        countSql = `
          WITH iris AS (
            SELECT geom_2154
            FROM decoupages.iris_grandeetendue_2022
            WHERE code_iris = $1::text
          )
          SELECT COUNT(*) AS total
          FROM equipements.base_2024 b, iris i
          WHERE b.typequ = ANY($2)
            AND ST_DWithin(b.geom_2154, i.geom_2154, $3);
        `;
        params = [codeIris, codes, dist];
      }
      const { rows: list }   = await pool.query(listSql, params);
      const { rows: counts } = await pool.query(countSql, params);
      commerces[prefix][rayon] = {
        count: parseInt(counts[0]?.total || 0, 10),
        items: list.map(r => ({ nom: r.nom, adresse: r.adresse }))
      };
    }
  }

  return commerces;
}

// --- Scores d'équipements en 1 requête ---------------------------------
async function fetchEquipScores(codeIris) {
  const q = `
    SELECT
      boulang_score  AS score_boulang,
      bouche_score   AS score_bouche,
      superm_score   AS score_superm,
      epicerie_score AS score_epicerie,
      lib_score      AS score_lib,
      cinema_score   AS score_cinema,
      conserv_score  AS score_conserv,
      magbio_score   AS score_magbio
    FROM equipements.iris_equip_2024
    WHERE code_iris = $1
    LIMIT 1
  `;
  const { rows } = await pool.query(q, [codeIris]);
  return rows[0] || {};
}

// ------------------------------------------------------------------
// POST /get_iris_filtre  (version LITE : rapide, sans hydratation)
// ------------------------------------------------------------------
app.post('/get_iris_filtre', async (req, res) => {
  console.log('>>> BODY RECEIVED FROM BUBBLE:', JSON.stringify(req.body, null, 2));
  console.time('TOTAL /get_iris_filtre_lite');

  try {
    const { mode, codes_insee, center, radius_km, criteria = {}, iris_base } = req.body;

    // 0) Récupération de la liste initiale d’IRIS (localisation)
    let irisSet = [];

    // Bypass : si Bubble envoie déjà la base d’IRIS
    if (Array.isArray(iris_base) && iris_base.length) {
      console.log(`🔄 Bypass localisation : ${iris_base.length} IRIS reçus`);
      irisSet = iris_base.map(String);

    } else if (mode === 'collectivites') {
      // Convertir départements/communes en codes communes finaux
      const selectedLocalities = (codes_insee || []).map(code => ({
        code_insee: code,
        type_collectivite: looksLikeDepartement(code) ? 'Département' : 'commune'
      }));
      const communesFinal = await gatherCommuneCodes(selectedLocalities);

      if (communesFinal.length) {
        const sql = `
          SELECT code_iris
          FROM decoupages.iris_grandeetendue_2022
          WHERE insee_com = ANY($1)
        `;
        const { rows } = await pool.query(sql, [communesFinal]);
        irisSet = rows.map(r => r.code_iris);
      }

    } else if (mode === 'rayon') {
      if (!center || center.lon == null || center.lat == null) {
        console.timeEnd('TOTAL /get_iris_filtre_lite');
        return res.status(400).json({ error: 'lon and lat are required for rayon mode' });
      }
      const radius_m = Number(radius_km) * 1000;
      const sql = `
        SELECT code_iris
        FROM decoupages.iris_grandeetendue_2022
        WHERE ST_DWithin(
          geom_2154,
          ST_Transform(ST_SetSRID(ST_MakePoint($1,$2),4326),2154),
          $3
        )
      `;
      const { rows } = await pool.query(sql, [center.lon, center.lat, radius_m]);
      irisSet = rows.map(r => r.code_iris);

    } else {
      console.timeEnd('TOTAL /get_iris_filtre_lite');
      return res.status(400).json({ error: 'mode invalid' });
    }

    if (!irisSet.length) {
      console.timeEnd('TOTAL /get_iris_filtre_lite');
      return res.json({ nb_iris: 0, iris: [] });
    }

    // 1) Application des critères SANS hydratation (on ne garde que le set d’IRIS)
    const applyIf = async (fn, active, ...args) => active ? (await fn(...args)).irisSet : args[0];

    // DVF
    irisSet = await applyIf(applyDVF, isDVFActivated(criteria?.dvf), irisSet, criteria.dvf);

    // Revenus / niveau de vie
    irisSet = await applyIf(applyRevenus, isRevenusActivated(criteria?.filosofi), irisSet, criteria.filosofi);

    // Logements sociaux (si critère utilisé)
    irisSet = await applyIf(applyLogSoc, isLogSocActivated(criteria?.filosofi), irisSet, criteria.filosofi);

    // Prix médian m² (si borne fournie dans criteria.prixMedianM2)
    if (criteria?.prixMedianM2 && (criteria.prixMedianM2.min != null || criteria.prixMedianM2.max != null)) {
      irisSet = (await applyPrixMedian(irisSet, criteria.prixMedianM2)).irisSet;
    }

    // Écoles
    irisSet = await applyIf(applyEcolesRadius, isEcolesActivated(criteria?.ecoles), irisSet, criteria.ecoles);

    // Collèges
    irisSet = await applyIf(applyColleges, isCollegesActivated(criteria?.colleges), irisSet, criteria.colleges);

    // Crèches
    irisSet = await applyIf(applyCreches, isCrechesActivated(criteria?.creches), irisSet, criteria.creches);

    // Équipements (scores BPE)
    if (criteria?.equipements) {
      for (const prefix of EQUIP_PREFIXES) {
        if (criteria.equipements[prefix]) {
          irisSet = (await applyScoreEquip(irisSet, prefix, criteria.equipements[prefix])).irisSet;
          if (!irisSet.length) break;
        }
      }
    }

    // Sécurité
    if (criteria?.securite) {
      const secRes = await applySecurite(irisSet, criteria.securite);
      irisSet = secRes.irisSet;
    }

    if (!irisSet.length) {
      console.timeEnd('TOTAL /get_iris_filtre_lite');
      return res.json({ nb_iris: 0, iris: [] });
    }

// 2) Récupération LÉGÈRE des noms d’IRIS ET du nom de commune
const nameSql = `
  SELECT i.code_iris,
         i.nom_iris,
         c.nom AS nom_commune
  FROM decoupages.iris_grandeetendue_2022 i
  LEFT JOIN LATERAL (
    SELECT nom
    FROM decoupages.communes c
    WHERE c.insee_com = i.insee_com OR c.insee_arm = i.insee_com
    LIMIT 1
  ) c ON true
  WHERE i.code_iris = ANY($1)
  ORDER BY array_position($1::text[], i.code_iris)
`;
const { rows: r2 } = await pool.query(nameSql, [irisSet]);

const iris = r2.map(r => ({
  code_iris: r.code_iris,
  nom_iris: r.nom_iris,
  nom_commune: r.nom_commune || null
}));

console.timeEnd('TOTAL /get_iris_filtre_lite');
return res.json({ nb_iris: iris.length, iris });

  } catch (err) {
    console.error('Erreur /get_iris_filtre (lite):', err);
    console.timeEnd('TOTAL /get_iris_filtre_lite');
    return res.status(500).json({ error: 'server', details: err.message });
  }
});

// ------------------------------------------------------------------
// GET /iris/:code/bbox           (table iris_petiteetendue_2022, SRID 4326)
// ------------------------------------------------------------------
app.get('/iris/:code/bbox', async (req, res) => {
  const { code } = req.params;
  if (!code) return res.status(400).json({ error: 'Code IRIS requis' });

  const sql = `
    SELECT
      ST_XMin(geom) AS west,
      ST_YMin(geom) AS south,
      ST_XMax(geom) AS east,
      ST_YMax(geom) AS north,
      nom_iris
    FROM decoupages.iris_petiteetendue_2022
    WHERE code_iris = $1
    LIMIT 1
  `;

  try {
    const { rows } = await pool.query(sql, [code]);
    if (!rows.length) return res.status(404).json({ error: 'IRIS non trouvé' });

    const b = rows[0];
    res.json({
      code_iris: code,
      nom_iris : b.nom_iris,
      bbox     : [Number(b.west), Number(b.south), Number(b.east), Number(b.north)]
    });
  } catch (err) {
    console.error('Erreur /iris/:code/bbox :', err);
    res.status(500).json({ error: 'Erreur serveur' });
  }
});

// ------------------------------------------------------------------
// POST /get_iris_data   (renvoie TOUT pour 1 IRIS)
// Body attendu: { code_iris: "XXXXXXXXX" }  (toujours 1 code)
// ------------------------------------------------------------------
app.post('/get_iris_data', async (req, res) => {
  try {
    const code = String(req.body?.code_iris || '').trim();
    if (!code) return res.status(400).json({ error: 'code_iris requis' });

    // 1) Base "fiche" via ta logique existante (DVF, revenus, part_log_soc, sécurité, prix, crèches, scores, etc.)
    //    NB: on passe un tableau [code] à buildIrisDetail, on récupère le 1er (et seul) objet.
    const baseArr = await buildIrisDetail([code], /*criteria*/ {}, /*equipCriteria*/ {});
    const base = (Array.isArray(baseArr) && baseArr[0]) ? baseArr[0] : null;
    if (!base) return res.status(404).json({ error: 'IRIS non trouvé' });

    // 2) BBox 4326 depuis "iris_petiteetendue_2022" (écrase la bbox éventuelle)
    const bbox4326 = await fetchIrisBbox4326(code);

    // 3) HLM détaillé (tous champs)
    const hlm = await fetchHlmDetail(code);

    // 4) Écoles (multi-rayons)
    const ecoles = await fetchEcolesAllRayons(code);

    // 5) Commerces (même structure que /get_all_commerces)
    const commerces = await fetchCommercesAll(code);

    // 👉 NOUVEAU : 5bis) Scores équipements en 1 requête
    const equipScores = await fetchEquipScores(code);

    // 6) Assemblage final (1 seul objet, pas de nb_iris, pas de centroid)
    const out = {
      code_iris       : base.code_iris,
      nom_iris        : base.nom_iris,
      commune         : base.commune,           // {nom_commune, nom_dep, code_dep}
      dvf_count       : base.dvf_count ?? 0,
      dvf_count_total : base.dvf_count_total ?? 0,
      mediane_rev_decl: base.mediane_rev_decl ?? null,
      part_log_soc    : base.part_log_soc ?? null,    // conserve le champ synthétique
      securite        : base.securite ?? null,
      prix_median_m2  : base.prix_median_m2 ?? null,
      taux_creches    : base.taux_creches ?? null,
      // 🔁 Scores d'équipements (1 requête)
      score_boulang   : equipScores.score_boulang  ?? base.score_boulang  ?? null,
      score_bouche    : equipScores.score_bouche   ?? base.score_bouche   ?? null,
      score_superm    : equipScores.score_superm   ?? base.score_superm   ?? null,
      score_epicerie  : equipScores.score_epicerie ?? base.score_epicerie ?? null,
      score_lib       : equipScores.score_lib      ?? base.score_lib      ?? null,
      score_cinema    : equipScores.score_cinema   ?? base.score_cinema   ?? null,
      score_conserv   : equipScores.score_conserv  ?? base.score_conserv  ?? null,
      score_magbio    : equipScores.score_magbio   ?? base.score_magbio   ?? null,
      // BBox depuis petiteetendue
      bbox            : bbox4326 || [null, null, null, null],
      // Ajouts détaillés
      hlm,            // { nblspls, part_log_soc, txlsplai, txlsplus, txlspls, txlspli }
      ecoles,         // { "300":[...], "600":[...], "1000":[...], "2000":[...], "5000":[...] }
      commerces,       // { prefix: { in_iris:{count,items}, 300:{...}, 600:{...}, 1000:{...} }, magbio:{...} }
      colleges        : (Array.isArray(base.colleges) || base.colleges === 'hors-scope')
                        ? base.colleges
                        : []
    };

    return res.json(out);

  } catch (err) {
    console.error('Erreur /get_iris_data :', err);
    return res.status(500).json({ error: 'Erreur serveur', details: err.message });
  }
});

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
  console.log('Received /ping request');
  try {
    await pool.query('SELECT 1');
    console.log('Database query successful');
    res.json({ message: 'pong', db_status: 'ok', date: new Date() });
  } catch (e) {
    console.error('Error in /ping:', e);
    res.status(500).json({ message: 'pong', db_status: 'error', error: e.message });
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