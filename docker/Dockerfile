FROM apache/airflow:2.10.2-python3.11

# Copia o requirements.txt do projeto para dentro da imagem
COPY requirements.txt /requirements.txt

# Instala as dependências do projeto (nosso código de extração, etc)
# --user não é necessário aqui pois a imagem oficial já roda como usuário 'airflow'
RUN pip install --no-cache-dir -r /requirements.txt