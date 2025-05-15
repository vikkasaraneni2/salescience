FROM python:3.11-slim

# Create a non-root user for security
RUN useradd -m appuser

WORKDIR /app

# Install Python dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy the rest of the code
COPY . .

# Use the non-root user
USER appuser

EXPOSE 8000

# Use uvicorn for FastAPI, or change as needed for your app
CMD ["uvicorn", "api.app:app", "--host", "0.0.0.0", "--port", "8000"]
