INSERT INTO {{params.target}}(
    table_name,
    column_name,
    data_type,
    metadados_dh_movto
)
SELECT 
    table_name,
    column_name,
    data_type,
    datetime(metadados_dh_movto, 'America/Sao_Paulo') metadados_dh_movto
FROM {{params.source}};