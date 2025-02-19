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

  // Filtrer pour enlever toute valeur null
  // si dvf.propertyTypes est un array
  if (Array.isArray(dvf.propertyTypes)) {
    dvf.propertyTypes = dvf.propertyTypes.filter(pt => pt != null);
  }

  const hasType    = dvf.propertyTypes && dvf.propertyTypes.length > 0;

  const hasBudget  = dvf.budget && (
    (dvf.budget.min != null) || (dvf.budget.max != null)
  );
  const hasSurface = dvf.surface && (
    (dvf.surface.min != null) || (dvf.surface.max != null)
  );
  const hasRooms   = dvf.rooms && (
    (dvf.rooms.min != null)   || (dvf.rooms.max != null)
  );
  const hasYears   = dvf.years && (
    (dvf.years.min != null)   || (dvf.years.max != null)
  );

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

// Fonction qui va servir à récupérer les IRIS quand l'utilisateur sélectionne un arrondissemnet de Paris, Lyon ou Marseille
async function getArrondissementsForVilleGlobale(codeVille) {
  // codeVille ex. "75056"
  // On suppose que dans decoupages.communes, 
  //   - "insee_com = codeVille" 
  //   - "insee_arm" = arrondissements
  // On veut SELECT DISTINCT insee_arm
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
      if (loc.type_collectivite === "commune"
        && ["75056","69123","13055"].includes(loc.code_insee)
      ) {
        // => c'est Paris, Lyon, ou Marseille global
        // => On éclate en arrondissements
        let arrCodes = await getArrondissementsForVilleGlobale(loc.code_insee);
        allCodes.push(...arrCodes);
      } else {
       // Commune normale, ou "arrondissement" explicite
         allCodes.push(loc.code_insee);
      }
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
  console.time('E) Revenus: build query');
  
  // 1) Préparer la liste d’IRIS en entrée
  //    => si c’est vide, on sort directement
  if (!irisList.length) {
    return { irisSet: [], revenusByIris: {} };
  }

  // 2) On construit la requête
  let whereClauses = [];
  let vals = [];
  let idx = 1;

  // (A) Filtrer sur code_iris dans irisList
  whereClauses.push(`code_iris = ANY($${idx})`);
  vals.push(irisList);
  idx++;

  // (B) si user a défini un min...
  let doIntersection = false;  // indicateur si on fera intersection
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

  // 3) Exec
  console.time('E) Revenus: exec');
  let r = await pool.query(query, vals);
  console.timeEnd('E) Revenus: exec');
  
  // 4) Stocker
  let revenusByIris = {};
  let irisOK = [];
  for (let row of r.rows) {
    let ci = row.code_iris;
    let mv = row.mediane_rev_decl != null ? Number(row.mediane_rev_decl) : null;
    revenusByIris[ci] = { mediane_rev_decl: mv };

    // tous ceux qui figurent dans le résultat 
    // => si doIntersection=true, c’est qu’ils respectent min..max
    // => sinon on en tient compte aussi
    irisOK.push(ci);
  }

  // 5) Intersection
  let irisSet;
  if (doIntersection) {
    irisSet = intersectArrays(irisList, irisOK);
  } else {
    // => pas de min..max => on ne restreint pas
    irisSet = irisList;
  }

  return { irisSet, revenusByIris };
}


// --------------------------------------------------------------
// F) Filtrage Logements sociaux => intersection stricte
// --------------------------------------------------------------
async function applyLogSoc(irisList, lsCriteria) {
  // 1) 
  if (!irisList.length) return { irisSet: [], logSocByIris: {} };

  // 2) Build la requête
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
// H) Critère partiel Ecoles => intersection stricte si "in-scope"
// --------------------------------------------------------------
async function applyEcolesPartial(irisList, ecolesCrit) {
  console.time('E) EcolesPartial: activation?');
  if (!isEcolesActivated(ecolesCrit)) {
    console.timeEnd('E) EcolesPartial: activation?');
    // => non activé => on met "non-activé" pour tous
    let ecolesByIris = {};
    for (let iris of irisList) {
      ecolesByIris[iris] = "non-activé";
    }
    return ecolesByIris;
  }
  console.timeEnd('E) EcolesPartial: activation?');

  // -------------------------------
  // 1) Couverture => table "iris_rne_ipsecoles"
  // -------------------------------
  console.time('E) EcolesPartial: coverage');
  const qDistinct = `
    SELECT DISTINCT code_iris
    FROM education_ecoles.iris_rne_ipsecoles
  `;
  let rDistinct = await pool.query(qDistinct);
  let setCouvert = new Set(rDistinct.rows.map(rr => rr.code_iris));
  console.timeEnd('E) EcolesPartial: coverage');

  // => On divise en deux : subsetCouvert, subsetHors
  let subsetCouvert = irisList.filter(iris => setCouvert.has(iris));
  let subsetHors     = irisList.filter(iris => !setCouvert.has(iris));

  // => "hors-scope" pour ceux hors
  let ecolesByIris = {};
  for (let iris of subsetHors) {
    ecolesByIris[iris] = "hors-scope";
  }

  if (!subsetCouvert.length) {
    // Si aucun IRIS n’est couvert => on a terminé
    // => tout est "hors-scope"
    return ecolesByIris;
  }

  // -------------------------------
  // 2) Requête filtrée par ips_min, ips_max
  // -------------------------------
  console.time('E) EcolesPartial: build query');
  let whereClauses = [ `code_iris = ANY($1)` ];
  let vals         = [ subsetCouvert ];
  let idx          = 2;

  if (ecolesCrit.ips_min != null) {
    whereClauses.push(`ips >= $${idx}`);
    vals.push(ecolesCrit.ips_min);
    idx++;
  }
  if (ecolesCrit.ips_max != null) {
    whereClauses.push(`ips <= $${idx}`);
    vals.push(ecolesCrit.ips_max);
    idx++;
  }
  console.timeEnd('E) EcolesPartial: build query');

  const sql = `
    SELECT code_iris, code_rne, ips
    FROM education_ecoles.iris_rne_ipsecoles
    WHERE ${whereClauses.join(' AND ')}
  `;
  console.time('E) EcolesPartial: exec query');
  let rPivot = await pool.query(sql, vals);
  console.timeEnd('E) EcolesPartial: exec query');

  // Regrouper par IRIS
  let irisFoundSet = new Set();
  let mapEcoles = {};  // code_iris => array d’écoles
  for (let row of rPivot.rows) {
    let ci = row.code_iris;
    irisFoundSet.add(ci);
    if (!mapEcoles[ci]) {
      mapEcoles[ci] = [];
    }
    mapEcoles[ci].push({
      code_rne: row.code_rne,
      ips: Number(row.ips)
    });
  }

  // => IRIS couverts "pas" dans irisFoundSet => 0 écoles => intersection stricte => tu peux soit
  //    - exclure totalement l’IRIS
  //    - ou renvoyer un tableau vide. Cf. discussion 
  // On suppose que “Critère partiel” => on ne retire PAS l’IRIS : on renvoie []
  // Mais si tu veux *retirer* l’IRIS de la liste => modifie la boucle suivante.

  for (let iris of subsetCouvert) {
    if (!irisFoundSet.has(iris)) {
      // => 0 écoles => on renvoie []
      mapEcoles[iris] = [];
    }
  }

  // -------------------------------
  // 3) Remplir ecolesByIris
  // -------------------------------
  for (let iris of subsetCouvert) {
    // soit un tableau d’écoles, soit [] si none
    ecolesByIris[iris] = mapEcoles[iris] || [];
  }

  console.log(`=> EcolesPartial: coverage = ${subsetCouvert.length} IRIS, pivot = ${rPivot.rowCount} rows`);
  return ecolesByIris;
}


// --------------------------------------------------------------
// I) Critère partiel Collèges => intersection stricte si in-scope
// --------------------------------------------------------------
async function applyCollegesPartial(irisList, colCrit) {
  console.time('F) CollegesPartial: activation?');
  if (!isCollegesActivated(colCrit)) {
    console.timeEnd('F) CollegesPartial: activation?');
    let collegesByIris = {};
    for (let iris of irisList) {
      collegesByIris[iris] = "non-activé";
    }
    return collegesByIris;
  }
  console.timeEnd('F) CollegesPartial: activation?');

  // 1) Couverture => retire les IRIS dont le departement figure dans DEPS_MANQUANTS
  const DEPS_MANQUANTS = ['17','22','2A','29','2B','52','56'];
  console.time('F) CollegesPartial: coverage');
  const sqlCoverage = `
    SELECT code_iris, insee_dep
    FROM decoupages.iris_2022
    WHERE code_iris = ANY($1)
  `;
  let rCouv = await pool.query(sqlCoverage, [irisList]);
  console.timeEnd('F) CollegesPartial: coverage');

  let subsetCouvert = [];
  let subsetHors    = [];
  for (let row of rCouv.rows) {
    if (DEPS_MANQUANTS.includes(row.insee_dep)) {
      subsetHors.push(row.code_iris);
    } else {
      subsetCouvert.push(row.code_iris);
    }
  }

  let collegesByIris = {};
  for (let iris of subsetHors) {
    collegesByIris[iris] = "hors-scope";
  }

  if (!subsetCouvert.length) {
    return collegesByIris; // => tout est hors-scope
  }

  // 2) Appliquer la requête pivot + intersection
  console.time('F) CollegesPartial: build query');
  let whereClauses = [ `code_iris = ANY($1)` ];
  let vals         = [ subsetCouvert ];
  let idx          = 2;

  if (colCrit.valeur_figaro_min != null) {
    whereClauses.push(`niveau_college_figaro >= $${idx}`);
    vals.push(colCrit.valeur_figaro_min);
    idx++;
  }
  if (colCrit.valeur_figaro_max != null) {
    whereClauses.push(`niveau_college_figaro <= $${idx}`);
    vals.push(colCrit.valeur_figaro_max);
    idx++;
  }
  console.timeEnd('F) CollegesPartial: build query');

  const sqlPivot = `
    SELECT code_iris, code_rne, niveau_college_figaro
    FROM education_colleges.iris_rne_niveaucolleges
    WHERE ${whereClauses.join(' AND ')}
  `;
  console.time('F) CollegesPartial: exec query');
  let rPivot = await pool.query(sqlPivot, vals);
  console.timeEnd('F) CollegesPartial: exec query');

  // regroupement
  let irisFoundSet = new Set();
  let mapCols = {};
  for (let row of rPivot.rows) {
    let ci = row.code_iris;
    irisFoundSet.add(ci);
    if (!mapCols[ci]) mapCols[ci] = [];
    mapCols[ci].push({
      code_rne: row.code_rne,
      valeur_figaro: Number(row.niveau_college_figaro)
    });
  }

  // => IRIS non dans irisFoundSet => tableau vide
  //   (on ne retire pas l’IRIS => critère partiel)
  for (let iris of subsetCouvert) {
    if (!irisFoundSet.has(iris)) {
      mapCols[iris] = [];
    }
  }

  for (let iris of subsetCouvert) {
    collegesByIris[iris] = mapCols[iris] || [];
  }

  return collegesByIris;
}


// --------------------------------------------------------------
// J) gatherInsecuByIris => note d'insécurité de la commune
// --------------------------------------------------------------
async function gatherInsecuByIris(irisList) {
  if (!irisList.length) {
  return { insecuByIris: {}, irisNameByIris: {} };
}

  console.time('Insecu details: query');
  const q = `
    SELECT i.code_iris, i.nom_iris, i.insee_com,
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
  let irisNameByIris = {};
  for (let row of r.rows) {
    // On stocke un array, comme en V1 (même si IRIS = 1 commune)
    insecuByIris[row.code_iris] = [{
      insee: row.insee_com,
      nom_com: row.nom_com || '(commune inconnue)',
      note: row.note_sur_20 != null ? Number(row.note_sur_20) : null
      // => on NE MET PAS nom_iris ici
    }];

    // (B) On stocke le nom_iris dans un objet distinct
    irisNameByIris[row.code_iris] = row.nom_iris || '(iris inconnu)';
  }
  return { insecuByIris, irisNameByIris };
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
    let { insecuByIris, irisNameByIris } = await gatherInsecuByIris(irisAfterSoc);

    // 8) Construire le tableau final
    let irisFinalDetail = [];
    for (let iris of irisAfterSoc) {
      let dvf_count = dvfCountByIris[iris] || 0;
      let rev = revenusByIris[iris] || {};
      let soc = logSocByIris[iris] || {};
      let ecolesVal = ecolesByIris[iris] || [];
      let colsVal = collegesByIris[iris] || [];
      let insecuVal = insecuByIris[iris] || [];

      let nomIris = irisNameByIris[iris] || null;


      irisFinalDetail.push({
        code_iris: iris,
        nom_iris: nomIris,  // => On l’a qu’UNE SEULE FOIS (résolution du problème où il apparaissait à deux endroits)
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
