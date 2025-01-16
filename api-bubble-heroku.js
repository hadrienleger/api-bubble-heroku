/****************************************************
 * Fichier : api-bubble-heroku.js
 * 
 * Description :
 *   - Filtrage DVF, Filosofi, etc.
 *   - Intersection finale => intersectionSet
 *   - Bloc final : renvoyer le nb de carreaux et la liste .communes
 *     en jointure avec decoupages.communes 
 *     (OR c.insee_arm = e.insee).
 *   - Filtre sur e.insee = ANY($2) pour n’afficher
 *     que les communes voulues par l’utilisateur.
 ****************************************************/

const express = require('express');
const cors = require('cors');
const { Pool } = require('pg');

// Charger .env si dev
if (process.env.NODE_ENV !== 'production') {
  require('dotenv').config();
}

// Connexion PG
const pool = new Pool({
  connectionString: process.env.ZENMAP_DATABASE_URL,
  ssl: process.env.NODE_ENV === 'production'
    ? { rejectUnauthorized: false }
    : false
});

// Initialisation Express
const app = express();
app.use(cors());
app.use(express.json());

// Test route
app.get('/ping', (req, res) => {
  res.json({ message: 'pong', date: new Date() });
});

// Petite fonction d'intersection
function intersectArrays(arrA, arrB) {
  const setB = new Set(arrB);
  return arrA.filter(x => setB.has(x));
}

// Convertir départements -> communes, 
// incluant arrondissements, etc.
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
    let result = await pool.query(query, val);
    let communesDep = result.rows.map(r => r.commune);
    allCommunes = [...allCommunes, ...communesDep];
  }
  return Array.from(new Set(allCommunes));
}

// POST /get_carreaux_filtre
app.post('/get_carreaux_filtre', async (req, res) => {
  try {
    console.log('=== START /get_carreaux_filtre ===');

    const { params, criteria } = req.body;
    if (!params || !params.code_type || !params.codes) {
      return res.status(400).json({ error: 'Paramètres de localisation manquants.' });
    }
    const { code_type, codes } = params;

    // (A) Localiser communes
    let communesSelection = [];
    if (code_type === 'com') {
      communesSelection = codes;
    } else if (code_type === 'dep') {
      let allCom = [];
      for (let dep of codes) {
        let communesDep = await getCommunesFromDepartements([dep]);
        allCom = [...allCom, ...communesDep];
      }
      communesSelection = Array.from(new Set(allCom));
    } else {
      return res.status(400).json({ error: 'code_type doit être "com" ou "dep".' });
    }
    // ...
    if (communesSelection.length === 0) {
      return res.json({ nb_carreaux: 0, carreaux: [] });
    }

    // (B) Insecurite
    let communesFinal = communesSelection;
    if (criteria && criteria.insecurite && criteria.insecurite.min != null) {
      const queryIns = `
        SELECT insee_com
        FROM delinquance.notes_insecurite_geom_complet
        WHERE note_sur_20 >= $1
          AND insee_com = ANY($2)
      `;
      const valIns = [criteria.insecurite.min, communesSelection];
      let resIns = await pool.query(queryIns, valIns);
      let communesInsecOk = resIns.rows.map(r => r.insee_com);

      // intersection
      communesFinal = intersectArrays(communesFinal, communesInsecOk);
    }
    if (communesFinal.length === 0) {
      return res.json({ nb_carreaux: 0, carreaux: [] });
    }

    // (C) Récupérer la liste de carreaux
    const queryCarrLoc = `
      SELECT idinspire AS id_carre_200m
      FROM decoupages.grille200m_metropole
      WHERE insee_com && $1
    `;
    const valCarrLoc = [communesFinal];
    let resCarrLoc = await pool.query(queryCarrLoc, valCarrLoc);
    let arrayCarreLoc = resCarrLoc.rows.map(r => r.id_carre_200m);
    if (arrayCarreLoc.length === 0) {
      return res.json({ nb_carreaux: 0, carreaux: [] });
    }

    // On va stocker les sets
    let setsOfCarreaux = [arrayCarreLoc];

    // (D) DVF ...
    // EXACTEMENT COMME AVANT
    // ... propertyTypes => codtyploc, budget => whereClauses, etc.
    // (conserve la version code que tu avais)

    // (E) Filosofi ...
    // idem

    // (F) Colleges ...
    // idem

    // (G) Ecoles ...
    // idem

    // (H) Intersection finale
    let intersectionSet = setsOfCarreaux[0];
    for (let i=1; i<setsOfCarreaux.length; i++) {
      intersectionSet = intersectArrays(intersectionSet, setsOfCarreaux[i]);
      if (intersectionSet.length === 0) break;
    }

    let finalResponse = {
      nb_carreaux: intersectionSet.length,
      carreaux: intersectionSet
    };

    // (I) Communes regroupement
    // On veut n'afficher que les communes
    // qui sont dans 'communesFinal'.
    // + on gère "OR c.insee_arm = e.insee"
    // pour trouver l'arrondissement.
    if (intersectionSet.length > 0 && communesFinal.length>0) {
      const queryCommunes = `
        WITH final_ids AS (
          SELECT unnest($1::text[]) AS id
        ),
        expanded AS (
          SELECT unnest(g.insee_com) AS insee
          FROM decoupages.grille200m_metropole g
          JOIN final_ids f ON g.idinspire = f.id
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
      const communesResult = await pool.query(queryCommunes, [
        intersectionSet, 
        communesFinal
      ]);

      let communesData = communesResult.rows.map(row => ({
        insee_com: row.insee_com,
        nom_com: row.nom_com,
        insee_dep: row.insee_dep,
        nom_dep: row.nom_dep,
        nb_carreaux: Number(row.nb_carreaux)
      }));
      finalResponse.communes = communesData;
    }

    return res.json(finalResponse);

  } catch (error) {
    console.error('Erreur dans /get_carreaux_filtre :', error);
    return res.status(500).json({ error: error.message });
  }
});

// Lancement
const PORT = process.env.PORT || 3000;
app.listen(PORT, () => {
  console.log(`API démarrée sur le port ${PORT}`);
});
