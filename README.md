## Python test project
This is a test project showing how to call beautifulsoup4 as part of an workitem queue

# Build and run
Setup environment with micromamba and run using the following commands
```bash
micromamba create -y -n pythontest -f conda.yaml
# or
micromamba install -y -n pythontest -f conda.yaml
```
To run using default python installation, use the following commands
```bash
pip uninstall openiap-edge
python -m pip cache purge
pip install -t . -r  requirements.txt
python main.py 
```
github repository:

```
https://github.com/openiap/beautifulsouptest.git
```