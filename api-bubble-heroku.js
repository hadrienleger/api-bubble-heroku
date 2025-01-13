/****************************************************
 * Fichier : api-bubble-heroku.js
 * 
 * Utilisation en local :
 *   1) npm install
 *   2) npm start
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
// 1) Fonction utilitaire d'intersection de tableaux
// ---------------------------------------------------------------------
function intersectArrays(arrA, arrB) {
  const setB = new Set(arrB);
  return arrA.filter(x => setB.has(x));
}

// ---------------------------------------------------------------------
// 2) Fonction utilitaire : convertir liste de départements
//    en liste de communes (ou arrondissements pour Paris, Lyon, Marseille).
// ---------------------------------------------------------------------
async function getCommunesFromDepartements(depCodes) {
  // depCodes = ["75","13","69", ...]
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

  return Array.from(new Set(allCommunes)); // unique
}

// ---------------------------------------------------------------------
// 3) POST /get_carreaux_filtre
// ---------------------------------------------------------------------
app.post('/get_carreaux_filtre', async (req, res) => {
  console.log('=== START /get_carreaux_filtre ===');
  console.time('TOTAL /get_carreaux_filtre');

  try {
    // On récupère params et criteria
    const { params, criteria } = req.body;
    // params = { code_type, codes }
    // criteria = { insecurite, dvf, filosofi, colleges, ecoles }

    // ----------------------------------------------------
    // A) Récupération de la liste de communes
    // ----------------------------------------------------
    console.time('A) localiser communes');
    let communesSelection = [];
    if (!params || !params.code_type || !params.codes) {
      return res.status(400).json({ error: 'Paramètres de localisation manquants.' });
    }

    const { code_type, codes } = params;

    if (code_type === 'com') {
      communesSelection = codes;
    } else if (code_type === 'dep') {
      // Convertir départements -> communes
      communesSelection = [];
      let allCom = [];
      for (let dep of codes) {
        let communesDep = await getCommunesFromDepartements([dep]);
        allCom = [...allCom, ...communesDep];
      }
      communesSelection = Array.from(new Set(allCom));
    } else {
      return res.status(400).json({ error: 'code_type invalide, doit être "com" ou "dep".' });
    }
    console.log('=> communesSelection.length =', communesSelection.length);
    console.timeEnd('A) localiser communes');

    if (communesSelection.length === 0) {
      // aucune commune => on arrête
      console.timeEnd('TOTAL /get_carreaux_filtre');
      return res.json({ nb_carreaux: 0, carreaux: [] });
    }

    // ----------------------------------------------------
    // B) Filtre insécurité => intersection de communes
    // ----------------------------------------------------
    let communesFinal = communesSelection; 
    if (criteria && criteria.insecurite && criteria.insecurite.min != null) {
      console.time('B) insecurite query');
      const queryIns = `
        SELECT insee_com
        FROM delinquance.notes_insecurite_geom_complet
        WHERE note_sur_20 >= $1
      `;
      const valIns = [criteria.insecurite.min];
      const resIns = await pool.query(queryIns, valIns);
      console.timeEnd('B) insecurite query');

      let communesInsecOk = resIns.rows.map(r => r.insee_com);
      console.log('=> communesInsecOk.length =', communesInsecOk.length);

      // Intersection
      console.time('B) intersection communes insecurite');
      communesFinal = intersectArrays(communesFinal, communesInsecOk);
      console.timeEnd('B) intersection communes insecurite');
      console.log('=> communesFinal (after insecurite) =', communesFinal.length);
    }

    if (communesFinal.length === 0) {
      // plus aucune commune => 0 carreaux
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

    // On stocke ce set dans un array pour l'intersection finale
    let setsOfCarreaux = [];
    setsOfCarreaux.push(arrayCarreLoc);

    // ----------------------------------------------------
    // D) Critère DVF => criteria.dvf
    //    propertyTypes, budget, surface, rooms, years
    // ----------------------------------------------------
    if (criteria && criteria.dvf) {
      console.time('D) DVF build query');
      let whereClauses = [];
      let values = [];
      let idx = 1;

      // propertyTypes => [maison,appartement]
      if (criteria.dvf.propertyTypes && criteria.dvf.propertyTypes.length>0) {
        let codesDVF = [];
        if (criteria.dvf.propertyTypes.includes('maison')) codesDVF.push('111');
        if (criteria.dvf.propertyTypes.includes('appartement')) codesDVF.push('121');
        if (codesDVF.length>0) {
          whereClauses.push(`codtypbien = ANY($${idx})`);
          values.push(codesDVF);
          idx++;
        }
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

      if (whereClauses.length>0) {
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
      } else {
        console.timeEnd('D) DVF build query');
      }
    }

    // ----------------------------------------------------
    // E) Critère Filosofi => criteria.filosofi
    //    nv_moyen, part_log_soc
    // ----------------------------------------------------
    if (criteria && criteria.filosofi) {
      console.time('E) Filosofi');
      let whereFilo = [];
      let valFilo = [];
      let iF = 1;

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
    // F) Critère collèges => criteria.colleges
    //    table pivot : idcar200m_rne_niveaucolleges
    //    champ niveau_colleges_figaro
    // ----------------------------------------------------
    if (criteria && criteria.colleges) {
      console.time('F) Colleges');
      let wf = [];
      let vf = [];
      let ic = 1;

      if (criteria.colleges.valeur_figaro_min != null) {
        wf.push(`niveau_colleges_figaro >= $${ic}`);
        vf.push(criteria.colleges.valeur_figaro_min);
        ic++;
      }
      if (criteria.colleges.valeur_figaro_max != null) {
        wf.push(`niveau_colleges_figaro <= $${ic}`);
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
    // G) Critère écoles => criteria.ecoles
    //    table pivot : idcar200m_rne_ipsecoles => champ ips
    // ----------------------------------------------------
    if (criteria && criteria.ecoles) {
      console.time('G) Ecoles');
      let wE = [];
      let vE = [];
      let iE = 1;

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
      // aucun set => pas de filtrage => 0
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
    return res.json({
      nb_carreaux: intersectionSet.length,
      carreaux: intersectionSet
    });

  } catch (error) {
    console.error('Erreur dans /get_carreaux_filtre :', error);
    console.timeEnd('TOTAL /get_carreaux_filtre');
    return res.status(500).json({ error: error.message });
  }
});

// Démarrer le serveur
const PORT = process.env.PORT || 3000;
app.listen(PORT, () => {
  console.log(`API démarrée sur le port ${PORT}`);
});
