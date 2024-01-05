# llama-index



For "No module named 'pkg_resources'"

```
pipenv uninstall -y setuptools
pipenv install setuptools==39.1.0
```

If no architecture support issue on macOS, try this:
```
ARCHFLAGS="-arch x86_64" pipenv install
```

# Ref.

https://docs.llamaindex.ai/en/stable/examples/metadata_extraction/MetadataExtractionSEC.html


# Dockerizing

```
pipenv run pip3 freeze > requirements.txt
docker build -t ghcr.io/jinyoung/memento:v1 .
```