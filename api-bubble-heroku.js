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

// Fonction pour récupérer les ventes totales des IRIS filtrés, hors critères
async function getDVFCountTotal(irisList) {
  // 1) Si la liste est vide, on renvoie un objet vide
  if (!irisList.length) {
    return {};
  }

  // 2) Requête
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
/****************************************************
 * applyEcoles(irisList, ecolesCrit)
 * ---------------------------------
 *  - On récupère TOUTES les écoles pour chaque IRIS couvert,
 *    quel que soit le paramétrage. 
 *  - Si l’utilisateur a défini ips_min / ips_max, on
 *    applique un filtrage “intersection stricte” : on
 *    ne garde dans le set final que les IRIS ayant AU MOINS
 *    un établissement répondant à la plage IPS.
 *  - Sinon, on ne filtre pas davantage, on conserve tous
 *    les IRIS passés en entrée.
 *  - “hors-scope” si l’IRIS n’est pas du tout présent
 *    dans la table pivot (pas d’école).
 ****************************************************/
async function applyEcoles(irisList, ecolesCrit) {
  console.time('applyEcoles');

  // 1) S’il n’y a pas d’IRIS en entrée, on renvoie direct.
  if (!irisList.length) {
    console.timeEnd('applyEcoles');
    return {
      irisSet: [],
      ecolesByIris: {}
    };
  }

  // 2) Récupération de la “couverture”
  //    => IRIS couverts = IRIS pour lesquels on a
  //       AU MOINS un enregistrement dans la table pivot.
  console.time('Ecoles coverage');
  const qCoverage = `
    SELECT DISTINCT code_iris
    FROM education_ecoles.iris_rne_ipsecoles
    WHERE code_iris = ANY($1)
  `;
  let coverageRes = await pool.query(qCoverage, [irisList]);
  console.timeEnd('Ecoles coverage');

  // -> coverageSet = ensemble d’IRIS couverts
  let coverageSet = new Set(coverageRes.rows.map(r => r.code_iris));

  // subsetCouvert = IRIS couverts
  let subsetCouvert = irisList.filter(ci => coverageSet.has(ci));
  // subsetHors = IRIS non couverts => ecolesByIris= "hors-scope"
  let subsetHors = irisList.filter(ci => !coverageSet.has(ci));

  // On prépare un objet ecolesByIris
  let ecolesByIris = {};
  for (let ci of subsetHors) {
    ecolesByIris[ci] = "hors-scope";
  }

  // 3) Si subsetCouvert est vide => plus rien à faire
  if (!subsetCouvert.length) {
    console.timeEnd('applyEcoles');
    // => on renvoie tout en “hors-scope” => le set final = irisList ?
    //    Si on veut réellement “exclure” tout IRIS hors-scope,
    //    on pourrait renvoyer [] à la place.
    return {
      irisSet: subsetHors, // ou [] si tu veux
      ecolesByIris
    };
  }

  // 4) Extraire TOUTES les écoles de subsetCouvert
  //    => si ecolesCrit a ips_min..max => on filtrera plus bas
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

  // Pour “intersection stricte”, on repère les IRIS
  // qui ont au moins 1 établissement dans la fourchette.
  // => irisFoundSet
  let irisFoundSet = new Set();
  // On stocke aussi la liste complète d’écoles
  let mapEcoles = {}; // code_iris => array d'obj { code_rne, ips }

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

  // 5) Si doIntersection = true => on retire du set final
  //    tous les IRIS qui ne figurent pas dans irisFoundSet
  let finalSet;
  if (doIntersection) {
    finalSet = subsetCouvert.filter(ci => irisFoundSet.has(ci));
  } else {
    // => on ne fait PAS de filtrage => on garde tout
    finalSet = subsetCouvert;
  }

  // 6) Pour chaque IRIS du finalSet => ecolesByIris = liste d’écoles
  //    (ou [] s’il n’y a rien dans mapEcoles)
  for (let ci of finalSet) {
    ecolesByIris[ci] = mapEcoles[ci] || [];
  }

  // 7) On peut concaténer subsetHors => ceux qui sont “hors-scope”
  //    + finalSet => IRIS “in-scope”
  let irisFinal = finalSet.concat(subsetHors);

  console.log(`applyEcoles => coverageRes=${coverageRes.rowCount} pivotRes=${pivotRes.rowCount}`);
  console.timeEnd('applyEcoles');

  return {
    irisSet: irisFinal,
    ecolesByIris
  };
}

// --------------------------------------------------------------
// I) Critère partiel Collèges => intersection stricte si in-scope
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

  // 1) Couverture départements manquants
  const DEPS_MANQUANTS = ['17','22','2A','29','2B','52','56'];
  console.time('Colleges coverage');
  const sqlCov = `
    SELECT code_iris, insee_dep
    FROM decoupages.iris_2022
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
    // => tout le monde hors-scope => pas de pivot
    return {
      irisSet: subsetHors,
      collegesByIris
    };
  }

  // 2) Requête pivot
  console.time('Colleges pivot');
  let wPivot = [`code_iris = ANY($1)`];
  let vals = [subsetCouvert];
  let idx = 2;

  let doIntersection = false;
  if (colCrit && colCrit.valeur_figaro_min != null) {
    wPivot.push(`niveau_college_figaro >= $${idx}`);
    vals.push(colCrit.valeur_figaro_min);
    idx++;
    doIntersection = true;
  }
  if (colCrit && colCrit.valeur_figaro_max != null) {
    wPivot.push(`niveau_college_figaro <= $${idx}`);
    vals.push(colCrit.valeur_figaro_max);
    idx++;
    doIntersection = true;
  }

  const sqlPivot = `
    SELECT code_iris, code_rne,
           nom_college,               -- NOUVEAU
           niveau_note20_methode_lineaire  -- NOUVEAU
    FROM education_colleges.iris_rne_niveaucolleges
    WHERE ${wPivot.join(' AND ')}
  `;
  let pivotRes = await pool.query(sqlPivot, vals);
  console.timeEnd('Colleges pivot');

  // 3) Grouper par IRIS
  let irisFoundSet = new Set();
  let mapCols = {}; // code_iris => array d'obj { code_rne, valeur_figaro }
  for (let row of pivotRes.rows) {
    let ci = row.code_iris;
    irisFoundSet.add(ci);
    if (!mapCols[ci]) mapCols[ci] = [];
    mapCols[ci].push({
      code_rne: row.code_rne,
      nom_college: row.nom_college,
      note_sur_20: Number(row.niveau_note20_methode_lineaire)
    });
  }

  // 4) Intersection stricte si doIntersection
  let finalSet;
  if (doIntersection) {
    finalSet = subsetCouvert.filter(ci => irisFoundSet.has(ci));
  } else {
    finalSet = subsetCouvert;
  }

  // 5) Remplir collegesByIris
  for (let ci of finalSet) {
    collegesByIris[ci] = mapCols[ci] || [];
  }

  // => on concat hors-scope + finalSet
  let irisFinal = finalSet.concat(subsetHors);

  console.log(`applyColleges => coverageRes=${covRes.rowCount} pivotRes=${pivotRes.rowCount}`);
  console.timeEnd('applyColleges');

  return {
    irisSet: irisFinal,
    collegesByIris
  };
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
    SELECT i.code_iris, i.nom_iris, d.note_sur_20
    FROM decoupages.iris_2022 i
    LEFT JOIN delinquance.notes_insecurite_geom_complet d
      ON d.insee_com = i.insee_com
         OR d.insee_com = i.insee_arm  -- si besoin pour arrondissements
    WHERE i.code_iris = ANY($1)
  `;
  let r = await pool.query(q, [irisList]);
  console.timeEnd('Insecu details: query');

  let insecuByIris = {};
  let irisNameByIris = {};
  for (let row of r.rows) {
    // On ne renvoie plus que la note
    const noteValue = (row.note_sur_20 != null) ? Number(row.note_sur_20) : null;

    insecuByIris[row.code_iris] = [
      { note: noteValue }
    ];
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

    // 5) Ecoles => renvoie { irisSet, ecolesByIris }
    let { irisSet: irisAfterEco, ecolesByIris } = await applyEcoles(irisAfterSoc, criteria?.ecoles);
    if (!irisAfterEco.length) {
      // fin => pas d’IRIS
      return res.json({ nb_iris: 0, iris: [], communes: [] });
    }

    // 6) Colleges => renvoie { irisSet, collegesByIris }
    let { irisSet: irisAfterCols, collegesByIris } = await applyColleges(irisAfterEco, criteria?.colleges);
    if (!irisAfterCols.length) {
      return res.json({ nb_iris: 0, iris: [], communes: [] });
    }

    // 6 bis) Récupérer le nombre total DVF pour ces IRIS
    let dvfTotalByIris = await getDVFCountTotal(irisAfterCols);

    // 7) Récupérer la note insécurité => gatherInsecuByIris
    let { insecuByIris, irisNameByIris } = await gatherInsecuByIris(irisAfterCols);

    // 8) Construire le tableau final
    let irisFinalDetail = [];
    for (let iris of irisAfterCols) {
      let dvf_count = dvfCountByIris[iris] || 0;
      let dvf_count_total = dvfTotalByIris[iris] || 0;      // ventes totales (NOUVEAU)
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
        dvf_count_total,
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
