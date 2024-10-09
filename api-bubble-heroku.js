const express = require('express');
const { Pool } = require('pg');
const app = express();

const pool = new Pool({
  connectionString: process.env.DATABASE_URL,
  ssl: { rejectUnauthorized: false }
});

app.use(express.json());

app.post('/find-iris', async (req, res) => {
  const { latitude, longitude, radius } = req.body;
  
  try {
    const query = `
      SELECT code_iris, nom_iris
      FROM sources.iris_ign_2023
      WHERE ST_DWithin(
        geography(geom),
        ST_MakePoint($2, $1)::geography,
        $3 * 1000
      )
    `;
    
    console.time('query');
    const result = await pool.query(query, [
      parseFloat(latitude), 
      parseFloat(longitude), 
      parseFloat(radius)
    ]);
    console.timeEnd('query');

    const results = result.rows;
    const total_count = results.length;

    console.log(`Nombre total d'IRIS trouvés : ${total_count}`);

    res.json({
      total: total_count,
      results: results
    });
  } catch (err) {
    console.error('Erreur lors de la requête :', err);
    res.status(500).json({ error: 'Une erreur est survenue lors de la recherche des IRIS' });
  }
});

const PORT = process.env.PORT || 3000;
app.listen(PORT, () => console.log(`Serveur démarré sur le port ${PORT}`));
