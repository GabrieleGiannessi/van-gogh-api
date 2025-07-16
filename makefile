run: 
	uvicorn main:app --reload

exec: 
	docker compose build --no-cache
	docker compose up