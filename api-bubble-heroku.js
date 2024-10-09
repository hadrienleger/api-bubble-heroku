app.post('/find-iris', async (req, res) => {
  const { latitude, longitude, radius, page = 1, limit = 100 } = req.body;
  const offset = (page - 1) * limit;
  
  try {
    const query = `
      WITH iris_within AS (
        SELECT code_iris, nom_iris
        FROM sources.iris_ign_2023
        WHERE ST_DWithin(
          geom::geography,
          ST_SetSRID(ST_MakePoint($2, $1), 4326)::geography,
          $3 * 1000
        )
      )
      SELECT 
        (SELECT COUNT(*) FROM iris_within) as total_count,
        (SELECT json_agg(t) FROM (
          SELECT * FROM iris_within
          LIMIT $4 OFFSET $5
        ) t) as results
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

    const { total_count, results } = result.rows[0];

    console.log(`Nombre total d'IRIS trouvés : ${total_count}`);
    console.log(`Nombre d'IRIS dans cette page : ${results ? results.length : 0}`);

    res.json({
      page: page,
      limit: limit,
      total: parseInt(total_count),
      results: results || []
    });
  } catch (err) {
    console.error('Erreur lors de la requête :', err);
    res.status(500).json({ error: 'Une erreur est survenue lors de la recherche des IRIS' });
  }
});