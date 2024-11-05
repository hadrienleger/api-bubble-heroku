// api-bubble-heroku.js

const express = require('express');
const cors = require('cors');
const app = express();
const { Pool } = require('pg');

// Configuration de la connexion à la base de données PostgreSQL
const pool = new Pool({
  connectionString: process.env.DATABASE_URL || 'postgresql://postgres@localhost:5432/module_quartier', // Normalement pas de mot de passe en local
  ssl: process.env.DATABASE_URL ? { rejectUnauthorized: false } : false
});

app.use(express.json());
app.use(cors());

app.post('/get_iris', async (req, res) => {
  try {
    const { method, params, criteria } = req.body;

    let query = '';
    let values = [];
    let valueIndex = 1; // Pour gérer les paramètres positionnels

    // Construction de la clause WHERE pour les critères
    let whereClauses = [];
    
    // Gestion des critères supplémentaires
    if (criteria) {
      // Filtre sur les années
      if (criteria.years && criteria.years.length === 2) {
        whereClauses.push(`anneemut BETWEEN $${valueIndex} AND $${valueIndex + 1}`);
        values.push(criteria.years[0], criteria.years[1]);
        valueIndex += 2;
      }

      // Filtre sur le type de bien
      if (criteria.propertyTypes && criteria.propertyTypes.length > 0) {
        whereClauses.push(`codtypbien = ANY($${valueIndex})`);
        values.push(criteria.propertyTypes);
        valueIndex += 1;
      }

      // Filtre sur le budget
      if (criteria.budget && criteria.budget.length === 2) {
        whereClauses.push(`valeurfonc BETWEEN $${valueIndex} AND $${valueIndex + 1}`);
        values.push(criteria.budget[0], criteria.budget[1]);
        valueIndex += 2;
      }

      // Filtre sur la surface
      if (criteria.surface && criteria.surface.length === 2) {
        whereClauses.push(`sbati BETWEEN $${valueIndex} AND $${valueIndex + 1}`);
        values.push(criteria.surface[0], criteria.surface[1]);
        valueIndex += 2;
      }

      // Filtre sur le nombre de pièces
      if (criteria.rooms && criteria.rooms.length === 2) {
        whereClauses.push(`nbpprinc BETWEEN $${valueIndex} AND $${valueIndex + 1}`);
        values.push(criteria.rooms[0], criteria.rooms[1]);
        valueIndex += 2;
      }
    }

    // Clause WHERE initialisée
    let whereClause = whereClauses.length > 0 ? 'WHERE ' + whereClauses.join(' AND ') : '';

    if (method === 'circle') {
      // Méthode 1 : Recherche par cercle
      const { latitude, longitude, radius } = params;

      query = `
        SELECT DISTINCT code_iris_wgs84 AS code_iris
        FROM dvf_filtre.table_simplifiee
        ${whereClause ? whereClause + ' AND' : 'WHERE'} ST_DWithin(
          geomloc_wgs84::geography,
          ST_SetSRID(ST_MakePoint($${valueIndex + 1}, $${valueIndex})::geography, 4326),
          $${valueIndex + 2} * 1000
        )
      `;
      values.push(parseFloat(latitude), parseFloat(longitude), parseFloat(radius));
      valueIndex += 3;

    } else if (method === 'codes') {
      // Méthode 2 : Sélection par codes de communes ou départements
      const { code_type, codes } = params; // code_type = 'com' ou 'dep'

      if (code_type === 'com') {
        query = `
          SELECT DISTINCT code_iris_wgs84 AS code_iris
          FROM dvf_filtre.table_simplifiee
          ${whereClause ? whereClause + ' AND' : 'WHERE'} l_codinsee && $${valueIndex}
        `;
        values.push(codes);
        valueIndex += 1;
      } else if (code_type === 'dep') {
        query = `
          SELECT DISTINCT code_iris_wgs84 AS code_iris
          FROM dvf_filtre.table_simplifiee
          ${whereClause ? whereClause + ' AND' : 'WHERE'} coddep = ANY($${valueIndex})
        `;
        values.push(codes);
        valueIndex += 1;
      } else {
        return res.status(400).json({ error: 'Type de code invalide.' });
      }
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
