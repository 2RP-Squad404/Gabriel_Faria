# Tarefa: Criação de Script SQL para Análise de Campanhas e Compras

Gabriel Faria e Silva

30/08/2024

## Criando as tabelas e importando os dados
Em primeira instância, é necessário analisar os arquivos CSV e identificar qual seria o tipo de dado necessário para cada coluna do dataset. Com essa informação em mãos, é possível criar a tabela utilizando o hive:

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

Aqui a tabela `purchases` é criada, com suas respectivas colunas estando contidas dentro dos parênteses.

O texto seguindo a criação da tabela determina que seu conteúdo será carregado de um arquivo de texto que possui como separador de informações uma vírgula `,` - ou seja, um arquivo CSV (Comma Separated Values). Em seguida, uma propriedade da tabela - `TBLPROPERTIES = Table Properties` -  é alterada, para que, ao ler o arquivo, a primeira linha (que seria o header da tabela, ou seja, o nome das colunas) não seja lida.

Com a tabela devidamente criada, já é possível carregar as informações do CSV para dentro da tabela. O arquivo CSV foi armazenado em um bucket do MinIO chamado `warehouse`.

    LOAD DATA INPATH 's3a://warehouse/purchases_2023.csv' INTO TABLE purchases

## Consultas e CTE (Common Table Expression)
Para a maioria das consultas realizadas nas tabelas, foi-se utilizado o conceito de `CTE (Common Table Expression)`.

Com o comando `WITH`, realizo uma consulta na tabela principal, porém, nomeio essa consulta e ela fica armazenada como uma tabela temporária. Feito isso, posso realizar subconsultas mais específicas nessa consulta já realizada.

    WITH locations AS (
        SELECT 
            client_id, 
            purchase_location, 
            COUNT(*) AS total, 
            ROW_NUMBER() OVER (PARTITION BY client_id ORDER BY COUNT(*) DESC) AS most_used 
        FROM purchases 
        GROUP BY client_id, purchase_location 
        ORDER BY client_id
    )

No exemplo acima, por exemplo, o foco principal é Retornar o ambiente (local) de compra mais utilizado pelo cliente e, para isso, consulto na tabela principal `purchases` e nomeio essa consulta como `locations`.

Nela é retornado o id do cliente, o local de compra e a contagem total de linhas, que, quando agrupada pelo id do cliente e pelo local de compra, conta quanto cada cliente comprou em cada local diferente.

Porém o retorno não foi somente do local mais comprado, mas sim de todos os locais! Para resolver o problema, utilizo a função `ROW_NUMBER()` para numerar as linhas de acordo com seus valores. Essa numeração é individual para cada cliente (ou seja, reseta para cada um) pela função `PARTITION BY client_id` e os valores são ordenados de forma decrescente, a fim do maior valor (local mais usado) aparecer primeiro.

Como o local mais usado aparece primeiro, ele sempre vai receber o número `1` pela função `ROW_NUMBER()` e, para isso, realizo uma subconsulta na consulta realizada, pelo `SELECT` abaixo:

    SELECT 
        client_id, 
        purchase_location AS most_purchase_location 
    FROM locations
    WHERE most_used = 1 
    ORDER BY client_id

Essa mesma lógica se repete para praticamente todas as consultas realizadas.

## JOIN, alias e consulta final
Ao fim, foi gerado uma CTE para cada coluna requerida na consulta final - com exceção às colunas que retornam a data atual pela função `CURRENT_DATE`.

Portanto, fez-se necessário juntar todas essas consultas em uma só tabela, o que é feito pelo comando `JOIN`.

O comando `JOIN` une duas tabelas de acordo com um campo em comum. No nosso caso, todas eram interconectadas pelo campo `client_id`. A sintaxe ficaria dessa forma:

    SELECT 
        client_id 
    FROM tabela_1 
    JOIN tabela_2 
        ON tabela_1.client_id = tabela_2.client_id

Porém, para deixar o código mais suscinto, é possível dar um `alias` à tabela, servindo como um tipo de apelido:

    SELECT 
        client_id 
    FROM tabela_1 t1 
    JOIN tabela_2 t2 
        ON t1.client_id = t2.client_id

No exemplo acima, `t1` e `t2` representam, respectivamente, `tabela_1` e `tabela_2`.

As cláusulas de condição (`WHERE`) dão lugar ao `ON`, ou seja, também devem ser passadas para esse campo.