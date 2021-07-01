# Fluxo de dados para alimentar tabelas e dashboards de uma organização

- Para deploy em pod com execução diária;

- Conexão SQLServer (pyodbc), tratamento em Python (pandas) e distribuição via API (gspread);

- Log de erros na execução é enviado para o email configurado;

- O fluxo realiza 3 tentativas de execução.

Obs.: Dados sensíveis foram removidos.

