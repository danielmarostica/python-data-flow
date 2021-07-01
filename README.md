# Fluxo de dados para alimentar tabelas e dashboards de uma organização

## Contexto
Havia uma barreira de VPN que impedia o acesso aos dados para grande parte da organização. Além disso, não era possível realizar a transferência de dados do banco para ferramentas de business intelligence e planilhas Google. Sendo assim, desenvolvi esta aplicação para possibilitar a criação de fluxos através da biblioteca `pyodbc` e da API `gspread`. Os módulos foram sendo desenvolvidos conforme a demanda organizacional.

## Informações
- Para deploy em pod com execução diária;
- Conexão SQLServer (`pyodbc`), tratamento em Python (`pandas`) e distribuição via API (`gspread`);
- Queries SQL assumem a forma de strings dentro do código Python para a requisição do `pyodbc`;
- O log de eventuais erros na execução é enviado para o email configurado;
- O fluxo realiza 3 tentativas de execução.

## Observação
Dados sensíveis foram removidos.

