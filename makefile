run: 
	uvicorn main:app --reload

exec: 
	docker compose build --no-cache
	docker compose up --remove-orphans

requirements: 
	pip freeze > requirements.txt