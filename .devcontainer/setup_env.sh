cp test.env.sample test.env

docker compose build
docker compose up -d

pip install -e .

pip install -r dev_requirements.txt
