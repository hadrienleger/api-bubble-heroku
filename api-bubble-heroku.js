/****************************************************
 * Fichier : api-bubble-heroku.js
 * Usage local :
 *   - npm install
 *   - npm start
 ****************************************************/

const express = require('express');
const cors = require('cors');
const { Pool } = require('pg');

// ------------------------------
// Charger .env si on n'est pas en production
// ------------------------------
if (process.env.NODE_ENV !== 'production') {
  require('dotenv').config();
}

// ------------------------------
// Connexion PG
// ------------------------------
const pool = new Pool({
  connectionString: process.env.ZENMAP_DATABASE_URL,
  ssl: process.env.NODE_ENV === 'production' 
    ? { rejectUnauthorized: false }
    : false
});

// ------------------------------
// Initialisation Express
// ------------------------------
const app = express();
app.use(cors());
app.use(express.json());

// Petit endpoint de test
app.get('/ping', (req, res) => {
  res.json({ message: 'pong', date: new Date() });
});

// -----------------------------------------------------------------
// 1) Fonction utilitaire : intersection de tableaux (set logic)
// -----------------------------------------------------------------
function intersectArrays(arrA, arrB) {
  const setB = new Set(arrB);
  return arrA.filter(x => setB.has(x));
}

/***********************************************************
 * 2) Fonction utilitaire : Récupérer la liste de communes
 *    à partir d'une liste de départements
 *    
 *    Gère Paris (75), Lyon (69), Marseille (13).
 *    => Pour ces villes (insee_com = 75056, 69123, 13055),
 *       on récupère insee_arm au lieu de insee_com.
 ***********************************************************/
async function getCommunesFromDepartements(depCodes) {
  // depCodes = ["75","13","69", ...]
  // On va collecter toutes les communes (ou arrondissements)
  // dans un seul array final "allCommunes".
  let allCommunes = [];

  for (let dep of depCodes) {
    // Pour chaque département, on va chercher
    // la table decoupages.communes : 
    // - si c.insee_dep = dep
    // - si c.insee_arm <> '' => on prend insee_arm
    // - sinon on prend insee_com

    // Sauf que Paris, Lyon, Marseille ont un petit twist :
    //   * Paris (75) => la grande commune est 75056,
    //                  mais arrondissements = insee_arm
    //   * Lyon  (69) => la commune Lyon est 69123
    //   * Marseille(13) => la commune Marseille est 13055
    //
    // Donc on fait un CASE WHEN pour remplacer insee_com par insee_arm
    // si c.insee_com est l'une de ces communes spéciales ?

    const query = `
      SELECT DISTINCT
        CASE 
          -- ex. pour Paris : si la commune est '75056'
          --  et que insee_arm <> '', alors on prend insee_arm
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

  // On supprime éventuellement les doublons (Set => array)
  let setComm = new Set(allCommunes);
  return Array.from(setComm);
}

/************************************************************
 * 3) /get_carreaux_filtre 
 * 
 *  - Récupère codes de communes ou départements
 *  - Gère insécurité si paramétré
 *  - Récupère la liste de carreaux
 *  - Applique DVF, Filosofi, Ecoles, Collèges
 *  - Intersection finale
 ************************************************************/
app.post('/get_carreaux_filtre', async (req, res) => {
  try {
    const { method, params, criteria } = req.body;

    // On va gérer un "setsOfCarreaux" final.
    // Chaque critère rajoute un set d'id_carre_200m
    // On intersectionne tout à la fin.
    let setsOfCarreaux = [];

    // -------------------------------------
    // A) DÉTERMINER la liste de COMMUNES
    //    en fonction de code_type = 'com' ou 'dep'
    // -------------------------------------
    let communesSelection = []; // liste de codes communes (ex. "75056","75017", or arrond "69381", etc.)
    if (method === 'codes') {
      const { code_type, codes } = params;
      // codes = ex. ["75056","75017"] si communes,
      //            ex. ["75","13"] si départements
      if (!code_type || !codes || codes.length === 0) {
        return res.status(400).json({ error: 'Paramètres localisation invalides.' });
      }

      if (code_type === 'com') {
        // Directement la liste de communes
        communesSelection = codes; 
      } 
      else if (code_type === 'dep') {
        // On doit convertir la liste de départements
        // en liste de communes. + Gérer Paris/Lyon/Marseille
        let allCom = [];
        for (let dep of codes) {
          // Récupérer les communes (ou arrond) associées
          // via la fonction getCommunesFromDepartements
          let communesDep = await getCommunesFromDepartements([dep]);
          allCom = [...allCom, ...communesDep];
        }
        // supprime doublons
        let setCom = new Set(allCom);
        communesSelection = Array.from(setCom);
      }
      else {
        return res.status(400).json({ error: 'code_type invalide, doit être com ou dep.' });
      }
    } 
    else {
      // On n'a plus de "circle" method.
      // si on n'a pas 'codes', on jette une erreur
      return res.status(400).json({ error: 'Méthode localisation non reconnue. (only codes)'});
    }

    // -------------------------------------
    // B) APPLIQUER un éventuel critère d’insécurité
    //    => intersection sur la liste de communes
    // -------------------------------------
    let communesFinal = communesSelection; // par défaut
    if (criteria && criteria.insecuriteMin != null) {
      // On récupère TOUTES les communes
      // dont note_sur_20 >= insecuriteMin
      const queryIns = `
        SELECT insee_com
        FROM delinquance.notes_insecurite_geom_complet
        WHERE note_sur_20 >= $1
      `;
      const valIns = [criteria.insecuriteMin];
      const resIns = await pool.query(queryIns, valIns);
      let communesInsecuOk = resIns.rows.map(r => r.insee_com);

      // On intersecte communesSelection et communesInsecuOk
      communesFinal = intersectArrays(communesSelection, communesInsecuOk);
    }

    // -------------------------------------
    // C) Récupérer la liste de carreaux 
    //    de grille200m_metropole 
    //    => insee_com && communesFinal
    // -------------------------------------
    if (communesFinal.length === 0) {
      // plus de communes => plus de carreaux => retour direct
      return res.json({
        nb_carreaux: 0,
        carreaux: []
      });
    } else {
      // On requête la table metropole
      const queryCarrLoc = `
        SELECT idinspire AS id_carre_200m
        FROM decoupages.grille200m_metropole
        WHERE insee_com && $1
      `;
      const valCarrLoc = [communesFinal];
      const resCarrLoc = await pool.query(queryCarrLoc, valCarrLoc);
      let arrayCarreLoc = resCarrLoc.rows.map(r => r.id_carre_200m);

      setsOfCarreaux.push(arrayCarreLoc);
    }

    // -------------------------------------
    // D) Critère DVF
    // -------------------------------------
    let whereClauses = [];
    let values = [];
    let idx = 1;

    if (criteria) {
      // propertyTypes => ["maison","appartement"]
      if (criteria.propertyTypes && criteria.propertyTypes.length>0) {
        let codesDVF = [];
        if (criteria.propertyTypes.includes('maison')) codesDVF.push('111');
        if (criteria.propertyTypes.includes('appartement')) codesDVF.push('121');
        if (codesDVF.length>0) {
          whereClauses.push(`codtypbien = ANY($${idx})`);
          values.push(codesDVF);
          idx++;
        }
      }
      // budget => {min, max} => valeurfonc
      if (criteria.budget) {
        if (criteria.budget.min != null) {
          whereClauses.push(`valeurfonc >= $${idx}`);
          values.push(criteria.budget.min);
          idx++;
        }
        if (criteria.budget.max != null) {
          whereClauses.push(`valeurfonc <= $${idx}`);
          values.push(criteria.budget.max);
          idx++;
        }
      }
      // surface => {min, max} => sbati
      if (criteria.surface) {
        if (criteria.surface.min != null) {
          whereClauses.push(`sbati >= $${idx}`);
          values.push(criteria.surface.min);
          idx++;
        }
        if (criteria.surface.max != null) {
          whereClauses.push(`sbati <= $${idx}`);
          values.push(criteria.surface.max);
          idx++;
        }
      }
      // rooms => nbpprinc
      if (criteria.rooms) {
        if (criteria.rooms.min != null) {
          whereClauses.push(`nbpprinc >= $${idx}`);
          values.push(criteria.rooms.min);
          idx++;
        }
        if (criteria.rooms.max != null) {
          whereClauses.push(`nbpprinc <= $${idx}`);
          values.push(criteria.rooms.max);
          idx++;
        }
      }
      // years => anneemut
      if (criteria.years) {
        if (criteria.years.min != null) {
          whereClauses.push(`anneemut >= $${idx}`);
          values.push(criteria.years.min);
          idx++;
        }
        if (criteria.years.max != null) {
          whereClauses.push(`anneemut <= $${idx}`);
          values.push(criteria.years.max);
          idx++;
        }
      }
    }
    if (whereClauses.length>0) {
      const whDVF = 'WHERE ' + whereClauses.join(' AND ');
      const queryDVF = `
        SELECT DISTINCT id_carre_200m
        FROM dvf_filtre.dvf_simplifie
        ${whDVF}
      `;
      let resDVF = await pool.query(queryDVF, values);
      let arrDVF = resDVF.rows.map(r => r.id_carre_200m);
      setsOfCarreaux.push(arrDVF);
    }

    // -------------------------------------
    // E) Critère Filosofi
    // -------------------------------------
    if (criteria && criteria.filosofi) {
      let whereFilo = [];
      let valFilo = [];
      let iF = 1;

      // nv_moyen => {min, max}
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
      // part_log_soc => {min, max}
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
        setsOfCarreaux.push(arrFi);
      }
    }

    // -------------------------------------
    // F) Critères collèges
    //    => table pivot : idcar200m_rne_classement_2023
    //    => champ valeur_avec_va_figaro
    // -------------------------------------
    if (criteria && criteria.colleges) {
      let wf = [];
      let vf = [];
      let ic = 1;
      if (criteria.colleges.valeur_figaro_min != null) {
        wf.push(`valeur_avec_va_figaro >= $${ic}`);
        vf.push(criteria.colleges.valeur_figaro_min);
        ic++;
      }
      if (criteria.colleges.valeur_figaro_max != null) {
        wf.push(`valeur_avec_va_figaro <= $${ic}`);
        vf.push(criteria.colleges.valeur_figaro_max);
        ic++;
      }
      if (wf.length>0) {
        const queryColl = `
          SELECT DISTINCT id_carre_200m
          FROM education_colleges.idcar200m_rne_classement_2023
          WHERE ${wf.join(' AND ')}
        `;
        let resColl = await pool.query(queryColl, vf);
        let arrColl = resColl.rows.map(r => r.id_carre_200m);
        setsOfCarreaux.push(arrColl);
      }
    }

    // -------------------------------------
    // G) Critères écoles
    //    => table pivot : idcar200m_rne_ipsecoles
    //    => champ ips
    // -------------------------------------
    if (criteria && criteria.ecoles) {
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
        setsOfCarreaux.push(arrEco);
      }
    }

    // -------------------------------------
    // H) Intersection finale
    // -------------------------------------
    if (setsOfCarreaux.length === 0) {
      // signifie qu'on n'a fait aucun filtre (?)
      // On pourrait renvoyer TOUT ou bien 0
      return res.json({
        nb_carreaux: 0,
        carreaux: [],
        info: "Pas de critères => pas de filtrage => 0"
      });
    } else {
      // intersection successives
      let intersectionSet = setsOfCarreaux[0];
      for (let i=1; i<setsOfCarreaux.length; i++) {
        intersectionSet = intersectArrays(intersectionSet, setsOfCarreaux[i]);
        if (intersectionSet.length===0) break;
      }

      // on renvoie tout (pas de limit)
      res.json({
        nb_carreaux: intersectionSet.length,
        carreaux: intersectionSet
      });
    }
    
  } catch (error) {
    console.error('Erreur dans /get_carreaux_filtre :', error);
    res.status(500).json({ error: error.message });
  }
});

// Démarrer le serveur
const PORT = process.env.PORT || 3000;
app.listen(PORT, () => {
  console.log(`API démarrée sur le port ${PORT}`);
});
