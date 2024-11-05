// api-bubble-heroku.js

const express = require('express');
const cors = require('cors');
const app = express();
const { Pool } = require('pg');

// Charger les variables d'environnement en développement
if (process.env.NODE_ENV !== 'production') {
  require('dotenv').config();
}

// Configuration de la connexion à la base de données PostgreSQL
const pool = new Pool({
  connectionString: process.env.DATABASE_URL,
  ssl: process.env.NODE_ENV === 'production' ? { rejectUnauthorized: false } : false
});

app.use(express.json());
app.use(cors());

app.post('/get_iris', async (req, res) => {
  try {
    const { method, params, criteria } = req.body;

    let query = '';
    let values = [];
    let valueIndex = 1; // Pour gérer les paramètres positionnels

    // Construction des clauses WHERE pour les critères supplémentaires
    let criteriaWhereClauses = [];

    // Gestion des critères (budget, type de bien, etc.)
    if (criteria) {
      // Filtre sur les années
      if (criteria.years) {
        if (criteria.years.min != null && criteria.years.max != null) {
          criteriaWhereClauses.push(`anneemut BETWEEN $${valueIndex} AND $${valueIndex + 1}`);
          values.push(criteria.years.min, criteria.years.max);
          valueIndex += 2;
        } else if (criteria.years.min != null) {
          criteriaWhereClauses.push(`anneemut >= $${valueIndex}`);
          values.push(criteria.years.min);
          valueIndex += 1;
        } else if (criteria.years.max != null) {
          criteriaWhereClauses.push(`anneemut <= $${valueIndex}`);
          values.push(criteria.years.max);
          valueIndex += 1;
        }
      }

      // Filtre sur le type de bien
      if (criteria.propertyTypes && criteria.propertyTypes.length > 0) {
        criteriaWhereClauses.push(`codtypbien = ANY($${valueIndex})`);
        values.push(criteria.propertyTypes);
        valueIndex += 1;
      }

      // Filtre sur le budget
      if (criteria.budget) {
        if (criteria.budget.min != null && criteria.budget.max != null) {
          criteriaWhereClauses.push(`valeurfonc BETWEEN $${valueIndex} AND $${valueIndex + 1}`);
          values.push(criteria.budget.min, criteria.budget.max);
          valueIndex += 2;
        } else if (criteria.budget.min != null) {
          criteriaWhereClauses.push(`valeurfonc >= $${valueIndex}`);
          values.push(criteria.budget.min);
          valueIndex += 1;
        } else if (criteria.budget.max != null) {
          criteriaWhereClauses.push(`valeurfonc <= $${valueIndex}`);
          values.push(criteria.budget.max);
          valueIndex += 1;
        }
      }

      // Filtre sur la surface
      if (criteria.surface) {
        if (criteria.surface.min != null && criteria.surface.max != null) {
          criteriaWhereClauses.push(`sbati BETWEEN $${valueIndex} AND $${valueIndex + 1}`);
          values.push(criteria.surface.min, criteria.surface.max);
          valueIndex += 2;
        } else if (criteria.surface.min != null) {
          criteriaWhereClauses.push(`sbati >= $${valueIndex}`);
          values.push(criteria.surface.min);
          valueIndex += 1;
        } else if (criteria.surface.max != null) {
          criteriaWhereClauses.push(`sbati <= $${valueIndex}`);
          values.push(criteria.surface.max);
          valueIndex += 1;
        }
      }

      // Filtre sur le nombre de pièces
      if (criteria.rooms) {
        if (criteria.rooms.min != null && criteria.rooms.max != null) {
          criteriaWhereClauses.push(`nbpprinc BETWEEN $${valueIndex} AND $${valueIndex + 1}`);
          values.push(criteria.rooms.min, criteria.rooms.max);
          valueIndex += 2;
        } else if (criteria.rooms.min != null) {
          criteriaWhereClauses.push(`nbpprinc >= $${valueIndex}`);
          values.push(criteria.rooms.min);
          valueIndex += 1;
        } else if (criteria.rooms.max != null) {
          criteriaWhereClauses.push(`nbpprinc <= $${valueIndex}`);
          values.push(criteria.rooms.max);
          valueIndex += 1;
        }
      }
    }

    // Selon la méthode choisie, on applique d'abord le critère de localisation
    if (method === 'circle') {
      // Méthode 1 : Recherche par cercle
      const { latitude, longitude, radius } = params;

      // Critère de localisation
      const locationCondition = `ST_DWithin(
        geomloc_wgs84::geography,
        ST_SetSRID(ST_MakePoint($${valueIndex + 1}, $${valueIndex}), 4326)::geography,
        $${valueIndex + 2} * 1000
      )`;
      values.push(parseFloat(latitude), parseFloat(longitude), parseFloat(radius));
      valueIndex += 3;

      // Sous-requête pour le critère de localisation
      query = `
        WITH location_filtered AS (
          SELECT *
          FROM dvf_filtre.table_simplifiee
          WHERE ${locationCondition}
        )
        SELECT DISTINCT code_iris_wgs84 AS code_iris
        FROM location_filtered
        ${criteriaWhereClauses.length > 0 ? 'WHERE ' + criteriaWhereClauses.join(' AND ') : ''}
      `;
    } else if (method === 'codes') {
      // Méthode 2 : Sélection par codes de communes ou départements
      const { code_type, codes } = params; // code_type = 'com' ou 'dep'

      let locationCondition = '';
      if (code_type === 'com') {
        locationCondition = `EXISTS (SELECT 1 FROM unnest(l_codinsee) AS code_insee WHERE code_insee = ANY($${valueIndex}))`;
        values.push(codes);
        valueIndex += 1;
      } else if (code_type === 'dep') {
        locationCondition = `coddep = ANY($${valueIndex})`;
        values.push(codes);
        valueIndex += 1;
      } else {
        return res.status(400).json({ error: 'Type de code invalide.' });
      }

      // Sous-requête pour le critère de localisation
      query = `
        WITH location_filtered AS (
          SELECT *
          FROM dvf_filtre.table_simplifiee
          WHERE ${locationCondition}
        )
        SELECT DISTINCT code_iris_wgs84 AS code_iris
        FROM location_filtered
        ${criteriaWhereClauses.length > 0 ? 'WHERE ' + criteriaWhereClauses.join(' AND ') : ''}
      `;
    } else {
      return res.status(400).json({ error: 'Méthode invalide.' });
    }

    // Jointure avec la table des IRIS pour obtenir les noms
    query = `
      WITH selected_iris AS (
        ${query}
      )
      SELECT si.code_iris, i.nom_iris, i.nom_com, i.insee_com
      FROM selected_iris si
      JOIN sources.iris_ign_2023 i ON si.code_iris = i.code_iris
    `;

    console.time('query');
    const result = await pool.query(query, values);
    console.timeEnd('query');

    const irisList = result.rows.map(row => ({
      code_iris: row.code_iris,
      nom_iris: row.nom_iris,
      nom_com: row.nom_com,
      insee_com: row.insee_com
    }));

    res.json({ iris: irisList });

  } catch (error) {
    console.error('Erreur lors de la requête :', error);
    res.status(500).json({ error: 'Erreur serveur.' });
  }
});

// Démarrer le serveur
const PORT = process.env.PORT || 3000;
app.listen(PORT, () => {
  console.log(`Serveur démarré sur le port ${PORT}`);
});
