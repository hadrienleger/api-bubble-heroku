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
// 1) Intersection utilitaire
// ---------------------------------------------------------------------
function intersectArrays(arrA, arrB) {
  const setB = new Set(arrB);
  return arrA.filter(x => setB.has(x));
}

// ---------------------------------------------------------------------
// 2) Convertir départements -> communes
//    Gérer arrondissements (75, 69, 13) si besoin
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
    allCommunes = [...allCommunes, ...communesDep];
  }

  return Array.from(new Set(allCommunes));
}

// ---------------------------------------------------------------------
// 3) POST /get_carreaux_filtre
// ---------------------------------------------------------------------
app.post('/get_carreaux_filtre', async (req, res) => {
  console.log('>>> BODY RECEIVED FROM BUBBLE:', JSON.stringify(req.body, null, 2));
  console.log('=== START /get_carreaux_filtre ===');
  console.time('TOTAL /get_carreaux_filtre');
  try {
    const { params, criteria } = req.body;
    if (!params || !params.code_type || !params.codes) {
      return res.status(400).json({ error: 'Paramètres de localisation manquants.' });
    }

    const { code_type, codes } = params;
    let communesSelection = [];

    // ----------------------------------------------------
    // A) Obtenir la liste de communes
    // ----------------------------------------------------
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
      return res.status(400).json({ error: 'code_type doit être "com" ou "dep".' });
    }
    console.log('=> communesSelection.length =', communesSelection.length);
    console.timeEnd('A) localiser communes');

    if (communesSelection.length === 0) {
      console.timeEnd('TOTAL /get_carreaux_filtre');
      return res.json({ nb_carreaux: 0, carreaux: [] });
    }

    // ----------------------------------------------------
    // B) Filtre insécurité -> intersection de communes
    // ----------------------------------------------------
    let communesFinal = communesSelection;
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
      communesFinal = intersectArrays(communesSelection, communesInsecOk);
      console.timeEnd('B) intersection communes insecurite');
      console.log('=> communesFinal (after insecurite) =', communesFinal.length);
    }

    if (communesFinal.length === 0) {
      console.timeEnd('TOTAL /get_carreaux_filtre');
      return res.json({ nb_carreaux: 0, carreaux: [] });
    }

    // ----------------------------------------------------
    // C) Récupérer la liste de carreaux => insee_com && communesFinal
    // ----------------------------------------------------
    console.time('C) grille200m query');
    const queryCarrLoc = `
      SELECT idinspire AS id_carre_200m
      FROM decoupages.grille200m_metropole
      WHERE insee_com && $1
    `;
    const valCarrLoc = [communesFinal];
    let resCarrLoc = await pool.query(queryCarrLoc, valCarrLoc);
    console.timeEnd('C) grille200m query');

    let arrayCarreLoc = resCarrLoc.rows.map(r => r.id_carre_200m);
    console.log('=> arrayCarreLoc.length =', arrayCarreLoc.length);

    if (arrayCarreLoc.length === 0) {
      console.timeEnd('TOTAL /get_carreaux_filtre');
      return res.json({ nb_carreaux: 0, carreaux: [] });
    }

    let setsOfCarreaux = [arrayCarreLoc];

    // ----------------------------------------------------
    // D) Critère DVF => filtrer dvf_simplifie 
    //    seulement sur id_carre_200m = ANY(arrayCarreLoc)
    // ----------------------------------------------------
    if (criteria && criteria.dvf) {
      console.time('D) DVF build query');
      let whereClauses = [];
      let values = [];
      let idx = 1;

      // 1) Filtrer sur id_carre_200m
      whereClauses.push(`id_carre_200m = ANY($${idx})`);
      values.push(arrayCarreLoc);
      idx++;

      // propertyTypes => Bubble enverra directement les valeurs numériques : 1 pour maison, 2 pour appartement
      if (criteria.dvf.propertyTypes && criteria.dvf.propertyTypes.length > 0) {
        // On suppose que criteria.dvf.propertyTypes est déjà un tableau de nombres : [1,2], [1], ou [2]
        whereClauses.push(`codtyploc = ANY($${idx})`);
        values.push(criteria.dvf.propertyTypes); // on push directement le tableau (ex. [1,2])
        idx++;
      }

      // budget
      if (criteria.dvf.budget) {
        if (criteria.dvf.budget.min != null) {
          whereClauses.push(`valeurfonc >= $${idx}`);
          values.push(criteria.dvf.budget.min);
          idx++;
        }
        if (criteria.dvf.budget.max != null) {
          whereClauses.push(`valeurfonc <= $${idx}`);
          values.push(criteria.dvf.budget.max);
          idx++;
        }
      }
      // surface
      if (criteria.dvf.surface) {
        if (criteria.dvf.surface.min != null) {
          whereClauses.push(`sbati >= $${idx}`);
          values.push(criteria.dvf.surface.min);
          idx++;
        }
        if (criteria.dvf.surface.max != null) {
          whereClauses.push(`sbati <= $${idx}`);
          values.push(criteria.dvf.surface.max);
          idx++;
        }
      }
      // rooms
      if (criteria.dvf.rooms) {
        if (criteria.dvf.rooms.min != null) {
          whereClauses.push(`nbpprinc >= $${idx}`);
          values.push(criteria.dvf.rooms.min);
          idx++;
        }
        if (criteria.dvf.rooms.max != null) {
          whereClauses.push(`nbpprinc <= $${idx}`);
          values.push(criteria.dvf.rooms.max);
          idx++;
        }
      }
      // years
      if (criteria.dvf.years) {
        if (criteria.dvf.years.min != null) {
          whereClauses.push(`anneemut >= $${idx}`);
          values.push(criteria.dvf.years.min);
          idx++;
        }
        if (criteria.dvf.years.max != null) {
          whereClauses.push(`anneemut <= $${idx}`);
          values.push(criteria.dvf.years.max);
          idx++;
        }
      }

      const whDVF = 'WHERE ' + whereClauses.join(' AND ');
      const queryDVF = `
        SELECT DISTINCT id_carre_200m
        FROM dvf_filtre.dvf_simplifie
        ${whDVF}
      `;
      console.timeEnd('D) DVF build query');

      console.time('D) DVF exec query');
      let resDVF = await pool.query(queryDVF, values);
      console.timeEnd('D) DVF exec query');

      let arrDVF = resDVF.rows.map(r => r.id_carre_200m);
      console.log('=> DVF rowCount =', arrDVF.length);
      setsOfCarreaux.push(arrDVF);
    }

    // ----------------------------------------------------
    // E) Filosofi => filtrer sur arrayCarreLoc + nv_moyen + part_log_soc
    // ----------------------------------------------------
    if (criteria && criteria.filosofi) {
      console.time('E) Filosofi');
      let whereFilo = [];
      let valFilo = [];
      let iF = 1;

      // 1) Filtrer par id_carre_200m
      whereFilo.push(`idcar_200m = ANY($${iF})`);
      valFilo.push(arrayCarreLoc);
      iF++;

      if (criteria.filosofi.nv_moyen) {
        if (criteria.filosofi.nv_moyen.min != null) {
          whereFilo.push(`nv_moyen >= $${iF}`);
          valFilo.push(criteria.filosofi.nv_moyen.min);
          iF++;
        }
        if (criteria.filosofi.nv_moyen.max != null) {
          whereFilo.push(`nv_moyen <= $${iF}`);
          valFilo.push(criteria.filosofi.nv_moyen.max);
          iF++;
        }
      }
      if (criteria.filosofi.part_log_soc) {
        if (criteria.filosofi.part_log_soc.min != null) {
          whereFilo.push(`part_log_soc >= $${iF}`);
          valFilo.push(criteria.filosofi.part_log_soc.min);
          iF++;
        }
        if (criteria.filosofi.part_log_soc.max != null) {
          whereFilo.push(`part_log_soc <= $${iF}`);
          valFilo.push(criteria.filosofi.part_log_soc.max);
          iF++;
        }
      }

      if (whereFilo.length>0) {
        const whFi = 'WHERE ' + whereFilo.join(' AND ');
        const queryFi = `
          SELECT idcar_200m
          FROM filosofi.c200_france_2019
          ${whFi}
        `;
        let resFi = await pool.query(queryFi, valFilo);
        let arrFi = resFi.rows.map(r => r.idcar_200m);
        console.log('=> Filosofi rowCount =', arrFi.length);
        setsOfCarreaux.push(arrFi);
      }
      console.timeEnd('E) Filosofi');
    }

    // ----------------------------------------------------
    // F) Collèges => filtrer sur arrayCarreLoc + niveau_college_figaro
    // ----------------------------------------------------
    if (criteria && criteria.colleges) {
      console.time('F) Colleges');
      let wf = [];
      let vf = [];
      let ic = 1;

      // Filtrer par id_carre_200m
      wf.push(`id_carre_200m = ANY($${ic})`);
      vf.push(arrayCarreLoc);
      ic++;

      if (criteria.colleges.valeur_figaro_min != null) {
        wf.push(`niveau_college_figaro >= $${ic}`);
        vf.push(criteria.colleges.valeur_figaro_min);
        ic++;
      }
      if (criteria.colleges.valeur_figaro_max != null) {
        wf.push(`niveau_college_figaro <= $${ic}`);
        vf.push(criteria.colleges.valeur_figaro_max);
        ic++;
      }

      if (wf.length>0) {
        const queryColl = `
          SELECT DISTINCT id_carre_200m
          FROM education_colleges.idcar200m_rne_niveaucolleges
          WHERE ${wf.join(' AND ')}
        `;
        let resColl = await pool.query(queryColl, vf);
        let arrColl = resColl.rows.map(r => r.id_carre_200m);
        console.log('=> Colleges rowCount =', arrColl.length);
        setsOfCarreaux.push(arrColl);
      }
      console.timeEnd('F) Colleges');
    }

    // ----------------------------------------------------
    // G) Écoles => filtrer sur arrayCarreLoc + ips
    // ----------------------------------------------------
    if (criteria && criteria.ecoles) {
      console.time('G) Ecoles');
      let wE = [];
      let vE = [];
      let iE = 1;

      wE.push(`id_carre_200m = ANY($${iE})`);
      vE.push(arrayCarreLoc);
      iE++;

      if (criteria.ecoles.ips_min != null) {
        wE.push(`ips >= $${iE}`);
        vE.push(criteria.ecoles.ips_min);
        iE++;
      }
      if (criteria.ecoles.ips_max != null) {
        wE.push(`ips <= $${iE}`);
        vE.push(criteria.ecoles.ips_max);
        iE++;
      }

      if (wE.length>0) {
        const queryEco = `
          SELECT DISTINCT id_carre_200m
          FROM education_ecoles.idcar200m_rne_ipsecoles
          WHERE ${wE.join(' AND ')}
        `;
        let resEco = await pool.query(queryEco, vE);
        let arrEco = resEco.rows.map(r => r.id_carre_200m);
        console.log('=> Ecoles rowCount =', arrEco.length);
        setsOfCarreaux.push(arrEco);
      }
      console.timeEnd('G) Ecoles');
    }

    // ----------------------------------------------------
    // H) Intersection finale
    // ----------------------------------------------------
    console.time('H) Intersection');
    if (setsOfCarreaux.length === 0) {
      console.timeEnd('H) Intersection');
      console.timeEnd('TOTAL /get_carreaux_filtre');
      return res.json({ nb_carreaux: 0, carreaux: [] });
    }

    let intersectionSet = setsOfCarreaux[0];
    for (let i=1; i<setsOfCarreaux.length; i++) {
      intersectionSet = intersectArrays(intersectionSet, setsOfCarreaux[i]);
      if (intersectionSet.length === 0) break;
    }
    console.timeEnd('H) Intersection');

    console.timeEnd('TOTAL /get_carreaux_filtre');

    // On construit la réponse principale
    let finalResponse = {
      nb_carreaux: intersectionSet.length,
      carreaux: intersectionSet
    };

    // I) Communes regroupement : renvoyer nb de carreaux par commune
    // On veut n'afficher que les communes qui sont dans 'communesFinal'.
    // + on gère "OR c.insee_arm = e.insee" pour trouver l'arrondissement.
    if (intersectionSet.length > 0 && communesFinal.length>0) {
      console.time('I) Communes regroupement');
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

      console.timeEnd('I) Communes regroupement');
      console.log('=> Nombre de communes distinctes =', communesResult.rowCount);

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
    console.timeEnd('TOTAL /get_carreaux_filtre');
    return res.status(500).json({ error: error.message });
  }
});

// Lancement serveur
const PORT = process.env.PORT || 3000;
app.listen(PORT, () => {
  console.log(`API démarrée sur le port ${PORT}`);
});
