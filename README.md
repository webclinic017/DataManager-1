## DataManager
This repo contains the logic to fetch stock data from yahoo finance and persist the data into a mysql table on aws

### Useful commands
Run `pip install -r requirements.txt` to install all the required libraries for the project.

Run `pip freeze > requirements.txt` to update requirements

#### To Run Tests
Use the command `python3 -m unittest test.test_TaWrapper` in the TexasHedge directory

Set the path to src to run the tests locally
`export PYTHONPATH=src`
