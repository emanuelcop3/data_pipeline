CREATE VIEW IF NOT EXISTS working_data_view AS
SELECT
    JSONExtract(dado_linha, 'preco','String') AS preco,
    JSONExtractInt(dado_linha, 'cod_prod') AS cod_prod
FROM working_data;