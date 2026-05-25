# Python container for dev/prod purpose

To help in the use of python for data development, 
this repo is here to give the Dockerfile/docker-compose to help in 
the build and deploy of data apps very easlisy.

Each folder contains one utility with one to many librairies to respond in the most use cases.
Also, they contains a README.md to explains how to start the docker-compose/Dockerfile and how to use it.

It will have many updates to follow the new trend with every librairies useful for Python dev like you.

# Which one is useful for you ?

If you want a general environment with the possibility to use many librairies like **Streamlit, Marimo, etc**,
we would advise you to use *python_script_dev_prod* which is the most generic one.


If you want a data app developed in Python, use *streamlit_dev_prod* or *dash_dev_prod* (only dashboard) for this use case.


If you want to scrap web data and store somewhere, you can use *scrapy_dev_prod* for this use case.


If you want a setup for your notebook, use *marimo_dev_prod*.


Finally, is you want to develop many pipelines and orchestrates them, you can use the *dagster_dbt_dev_prod* folder.

