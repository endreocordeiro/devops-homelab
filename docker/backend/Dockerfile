# 1️⃣ Imagem base
FROM python:3.12-slim

# 2️⃣ Diretório de trabalho dentro do container
WORKDIR /app

# 3️⃣ Copiar arquivo de dependências
COPY requirements.txt .

# 4️⃣ Instalar dependências
RUN pip install --no-cache-dir -r requirements.txt

# 5️⃣ Copiar código da aplicação
COPY app.py .

# 6️⃣ Expor porta
EXPOSE 5000

# 7️⃣ Comando padrão ao iniciar container
CMD ["python", "app.py"]