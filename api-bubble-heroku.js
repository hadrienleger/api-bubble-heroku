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
        geom::geography,
        ST_SetSRID(ST_MakePoint($2, $1), 4326)::geography,
        $3 * 1000
      );
    `;
    
    const result = await pool.query(query, [latitude, longitude, radius]);
    
    res.json(result.rows);
  } catch (err) {
    console.error(err);
    res.status(500).json({ error: 'Une erreur est survenue lors de la recherche des IRIS' });
  }
});

const PORT = process.env.PORT || 3000;
app.listen(PORT, () => console.log(`Serveur démarré sur le port ${PORT}`));

console.time('query');
const result = await pool.query(query, [latitude, longitude, radius]);
console.timeEnd('query');