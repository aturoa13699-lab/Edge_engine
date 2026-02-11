FROM python:3.12-slim

ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

# If you use Playwright scrapers in-container:
# RUN playwright install chromium

EXPOSE ${PORT:-8501}

CMD ["sh", "-c", "streamlit run streamlit_app/hud.py --server.address=0.0.0.0 --server.port=${PORT:-8501} --server.headless=true"]
