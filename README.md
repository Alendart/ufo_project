
![ufo_project_logo](https://github.com/user-attachments/assets/3e16710a-d3f5-4867-b012-deff66b31d45)

# UFO Analysis

Big data Analysis made for Merito University on 2024/2025. Cleaning, proccessing and analysing data 


## Authors

- [@alendart](https://www.github.com/alendart)
- [@ewajurek](https://www.github.com/ewajurek)
- [@kiekrz](https://www.github.com/kiekrz)
- [@Woicieszeq](https://www.github.com/Woicieszeq)
- [@wojtekowski](https://www.github.com/wojtekowski)




## Deployment

To set up proper enviroment follow below rules

- Install python 3.12 or higher
- Create folder on C:/Repos/ 
    (Can be ommited but then you need to repair paths to data files)
- Then clone repository
```bash
  git clone https://github.com/Alendart/ufo_project.git
```
- Advance to created folder and set up python enviroment
```bash
  cd ufo_project
  python -m venv .ufo
```
- Run python enviroment
```bash
  .ufo\Scripts\activate
```
- Install dependecies
```bash
  pip install datetime pandas jupyterlab pycountry openpyxl geopy scipy tqdm geopandas csvkit shapely pyproj
```
- Run Jupyter Lab 
```bash
  jupyter-lab
```

