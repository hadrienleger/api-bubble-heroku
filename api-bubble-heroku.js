// api-bubble-heroku.js
const express = require('express');
const cors = require('cors');
const app = express();
const { Pool } = require('pg');

// Configuration de la connexion à la base de données PostgreSQL
const pool = new Pool({
  connectionString: process.env.DATABASE_URL,
  ssl: { rejectUnauthorized: false }
});

app.use(express.json());
app.use(cors());

app.post('/get_iris', async (req, res) => {
  try {
    const { method, params } = req.body;

    let query = '';
    let values = [];

    if (method === 'circle') {
    // Méthode 1 : Recherche par cercle (Version n°2)
      const { latitude, longitude, radius } = params;

      query = `
        SELECT code_iris, nom_iris, nom_com
        FROM sources.iris_ign_2023
        WHERE ST_DWithin(
          geography(geom),
          ST_MakePoint($2, $1)::geography,
          $3 * 1000
        )
      `;
      values = [
        parseFloat(latitude),
        parseFloat(longitude),
        parseFloat(radius)
      ];

    } else if (method === 'codes') {
      // Méthode 2 : Sélection par codes de communes ou départements
      const { code_type, codes } = params; // code_type = 'com' ou 'dep'

      if (code_type === 'com') {
        query = `
          SELECT code_iris, nom_iris, nom_com
          FROM sources.iris_ign_2023
          WHERE com = ANY($1)
        `;
      } else if (code_type === 'dep') {
        query = `
          SELECT code_iris, nom_iris, nom_com
          FROM sources.iris_ign_2023
          WHERE dep = ANY($1)
        `;
      } else {
        return res.status(400).json({ error: 'Type de code invalide.' });
      }
      values = [codes];
    } else {
      return res.status(400).json({ error: 'Méthode invalide.' });
    }

    console.time('query');
    const result = await pool.query(query, values);
    console.timeEnd('query');

    const irisList = result.rows.map(row => ({
      code_iris: row.code_iris,
      nom_iris: row.nom_iris,
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
