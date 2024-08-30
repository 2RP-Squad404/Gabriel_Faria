--------------------------------
-- CONSULTAS NO CSV PURCHASES --
--------------------------------

CREATE TABLE purchases (
    purchase_id STRING,
    product_name STRING,
    product_id STRING,
    amount INT,
    price DOUBLE,
    discount_applied DOUBLE,
    payment_method STRING,
    purchase_datetime DATE,
    purchase_location STRING,
    client_id STRING
) ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' STORED AS TEXTFILE TBLPROPERTIES("skip.header.line.count"="1")

LOAD DATA INPATH 's3a://warehouse/purchases_2023.csv' INTO TABLE purchases

-- RETORNANDO O PREÇO TOTAL GASTO POR CLIENTE PELA FÓRMULA (price * amount * discount_applied) --
    -- Através da subconsulta "price_calc", retorno o id do cliente e a soma do total gasto para cada cliente - separação feita pela função "PARTITION BY client_id"
WITH price_calc AS (
    SELECT 
        client_id, 
        ROUND(SUM((price * amount * discount_applied)) OVER (PARTITION BY client_id),2) AS total_price 
    FROM purchases
)

SELECT 
    client_id, 
    total_price 
FROM price_calc 
GROUP BY client_id, total_price 
ORDER BY client_id

-- RETORNANDO O LOCAL DE COMPRA MAIS UTILIZADO POR CLIENTE --
    -- O "WITH" abaixo cria uma subconsulta, filtrando a tabela principal (purchases) na qual mais tarde realizarei consultas -> CONCEITO DE CTE (Common Table Expression)
    -- Para isso seleciono o id do cliente, os locais de compra, a soma de tudo e ordeno a tabela pelas duas colunas previamente mencionadas para separar a quantidade em que cada cliente comprou via cada local
    -- Por fim, utilizo a função ROW_NUMBER() - que, como o nome indica, numera as linhas. Essa numeração é feita com base em cada cliente separado (por isso a função PARTITION - a numeração reseta para cada cliente diferente) e essa numeração é ordenada de forma decrescente, baseando-se na contagem total de linhas (que basicamente é a quantidade em que cada usuário comprou em cada local de compra). Dessa forma, o local em que o usuário mais comprou recebe o número de linha 1.
WITH locations AS (
    SELECT 
        client_id, 
        purchase_location, 
        COUNT(*) AS total, ROW_NUMBER() OVER (PARTITION BY client_id ORDER BY COUNT(*) DESC) AS most_used 
    FROM purchases 
    GROUP BY client_id, purchase_location 
    ORDER BY client_id
)
    -- Se o local em que o usuário mais comprou recebe o número de linha 1, por fim, seleciono tudo da subconsulta onde o número de linha é apenas 1.
SELECT 
    client_id, 
    purchase_location AS most_purchase_location 
FROM locations 
WHERE most_used = 1 
ORDER BY client_id

-- RETORNANDO AS DATAS DAS PRIMEIRAS E ÚLTIMAS COMPRAS REALIZADAS POR CADA CLIENTE --
    -- Traz o id dos clientes, as datas em que compraram e numera as linhas - resetando a cada cliente pelo PARTITION - de acordo com as datas em que compraram em ordem crescente
    -- Traz também a contagem total de vezes em que o cliente apareceu na tabela (ou seja, que comprou algo) na coluna "total_dates".
WITH dates AS (
    SELECT 
        client_id, 
        purchase_datetime, 
        ROW_NUMBER() OVER (PARTITION BY client_id ORDER BY purchase_datetime) AS ordered_dates, 
        COUNT(*) OVER (PARTITION BY client_id) AS total_dates 
    FROM purchases 
    GROUP BY client_id, purchase_datetime 
    ORDER BY client_id
)

    -- Realiza uma consulta em "dates" criada anteriormente para trazer a última e primeira data de compra do usuário
    -- Para isso, clia dois "alias" (ou apelidos) para "dates": d1 e d2. d1 vai ser responsável por retornar a primeira data (onde, nas datas ordenadas, traz a primeira posição), enquanto d2 será responsável por retornar a última (nas datas ordenadas, traz a posição do total de vezes que aparece na tabela, ou seja, traz a última posição).
SELECT 
    d1.client_id AS client_id, 
    d1.purchase_datetime AS first_date, 
    d2.purchase_datetime AS last_date 
FROM dates d1 
JOIN dates d2 
    ON d1.client_id = d2.client_id 
    AND d1.ordered_dates = 1 
    AND d2.ordered_dates = d2.total_dates 
ORDER BY d1.client_id

-----------------------------------------------------------------------------------------------------------------------------------------

--------------------------------
-- CONSULTAS NO CSV CAMPAIGNS --
--------------------------------

CREATE TABLE campaigns (
    access_number INT,
    id_campaign INT,
    type_campaign STRING,
    days_valid INT,
    data_campaign DATE,
    channel STRING,
    return_status STRING,
    return_date DATE,
    client_id STRING
) ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' STORED AS TEXTFILE TBLPROPERTIES("skip.header.line.count"="1")

LOAD DATA INPATH 's3a://warehouse/campaigns_2023_hist.csv' INTO TABLE campaigns

-- RETORNA A CAMPANHA MAIS RECEBIDA POR USUÁRIO --
    -- Seleciona o id do cliente, o id da campanha, a contagem de todas as linhas (agrupado pelas colunas previamente mencionados)
        -- e numera as linhas (separadas por cliente pelo PARTITION BY) de acordo com a contagem das linhas em ordem decrescente.
    -- Como a contagem das linhas é agrupada pelo id da campanha, as campanhas são ranqueadas de acordo com quantas vezes elas foram recebidas
WITH received AS (
    SELECT 
        client_id, 
        id_campaign, 
        COUNT(*) AS times_received, ROW_NUMBER() OVER (PARTITION BY client_id ORDER BY COUNT(*) DESC) AS most_received 
    FROM campaigns 
    WHERE return_status = "received" 
    GROUP BY client_id, id_campaign
)

SELECT 
    client_id, 
    id_campaign AS most_campaign 
    FROM received 
    WHERE most_received = 1

-- RETORNA A QUANTIDADE DE ERROS RECEBIDOS POR USUÁRIO --
WITH error_quantities AS (
    SELECT 
        client_id, 
        COUNT(*) AS quantity_error 
    FROM campaigns 
    WHERE return_status = "error" 
    GROUP BY client_id 
    ORDER BY client_id
)

SELECT * FROM error_quantities

-- SELEÇÃO DAS DATAS ATUAIS --
SELECT CURRENT_DATE AS date_today

SELECT CAST(DATE_FORMAT(CURRENT_DATE, 'MMYYYY') AS INT) AS anomes_today

-----------------------------------------------------------------------------------------------------------------------------------------

-------------------------------------------------
-- SCRIPT COMPLETO (retorna a tabela desejada) --
-------------------------------------------------

WITH price_calc AS (
    SELECT 
        client_id, 
        ROUND(SUM((price * amount * discount_applied)) OVER (PARTITION BY client_id),2) AS total_price 
    FROM purchases
),

locations AS (
    SELECT 
        client_id, 
        purchase_location, 
        COUNT(*) AS total, 
        ROW_NUMBER() OVER (PARTITION BY client_id ORDER BY COUNT(*) DESC) AS most_used 
    FROM purchases 
    GROUP BY client_id, purchase_location 
    ORDER BY client_id
),

dates AS (
    SELECT 
        client_id, 
        purchase_datetime, 
        ROW_NUMBER() OVER (PARTITION BY client_id ORDER BY purchase_datetime) AS ordered_dates, 
        COUNT(*) OVER (PARTITION BY client_id) AS total_dates 
    FROM purchases 
    GROUP BY client_id, purchase_datetime 
    ORDER BY client_id
),

received AS (
    SELECT 
        client_id, 
        id_campaign, 
        COUNT(*) AS times_received, ROW_NUMBER() OVER (PARTITION BY client_id ORDER BY COUNT(*) DESC) AS most_received 
    FROM campaigns 
    WHERE return_status = "received" 
    GROUP BY client_id, id_campaign
),

error_quantities AS (
    SELECT 
        client_id, 
        COUNT(*) AS quantity_error 
    FROM campaigns 
    WHERE return_status = "error" 
    GROUP BY client_id
)

SELECT 
    loc.client_id AS client_id, 
    loc.purchase_location AS most_purchase_location, 
    d1.purchase_datetime AS first_date, 
    d2.purchase_datetime AS last_date,
    rec.id_campaign AS most_campaign,
    price.total_price AS total_price,
    error.quantity_error AS quantity_error,
    CURRENT_DATE as date_today,
    CAST(DATE_FORMAT(CURRENT_DATE, 'MMYYYY') AS INT) AS anomes_today
FROM locations loc 
JOIN dates d1 
    ON loc.client_id = d1.client_id 
    AND loc.most_used = 1
JOIN dates d2 
    ON d1.client_id = d2.client_id 
    AND d1.ordered_dates = 1 
    AND d2.ordered_dates = d2.total_dates 
JOIN received rec 
    ON d2.client_id = rec.client_id 
    AND rec.most_received = 1
JOIN price_calc price 
    ON rec.client_id = price.client_id
JOIN error_quantities error 
    ON price.client_id = error.client_id
GROUP BY loc.client_id, loc.purchase_location, d1.purchase_datetime, d2.purchase_datetime, rec.id_campaign, price.total_price, error.quantity_error
ORDER BY loc.client_id
