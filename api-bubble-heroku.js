const express = require('express');
const { Pool } = require('pg');
const app = express();

const pool = new Pool({
  connectionString: process.env.DATABASE_URL,
  ssl: { rejectUnauthorized: false }
});

app.use(express.json());

app.post('/find-iris', async (req, res) => {
  const { latitude, longitude, radius, page = 1, limit = 100 } = req.body;
  const offset = (page - 1) * limit;
  
  try {
    const query = `
      SELECT code_iris, nom_iris
      FROM sources.iris_ign_2023
      WHERE ST_DWithin(
        geom::geography,
        ST_SetSRID(ST_MakePoint($2, $1), 4326)::geography,
        $3 * 1000
      )
      LIMIT $4 OFFSET $5;
    `;
    
    console.time('query');
    const result = await pool.query(query, [
      parseFloat(latitude), 
      parseFloat(longitude), 
      parseFloat(radius),
      limit,
      offset
    ]);
    console.timeEnd('query');
    
    const count = await pool.query(`
      SELECT COUNT(*) 
      FROM sources.iris_ign_2023
      WHERE ST_DWithin(
        geom::geography,
        ST_SetSRID(ST_MakePoint($1, $2), 4326)::geography,
        $3 * 1000
      )
    `, [parseFloat(latitude), parseFloat(longitude), parseFloat(radius)]);

    console.log(`Nombre total d'IRIS trouvés : ${count.rows[0].count}`);
    console.log(`Nombre d'IRIS dans cette page : ${result.rows.length}`);

    res.json({
      page: page,
      limit: limit,
      total: parseInt(count.rows[0].count),
      results: result.rows
    });
  } catch (err) {
    console.error('Erreur lors de la requête :', err);
    res.status(500).json({ error: 'Une erreur est survenue lors de la recherche des IRIS' });
  }
});

const PORT = process.env.PORT || 3000;
app.listen(PORT, () => console.log(`Serveur démarré sur le port ${PORT}`));