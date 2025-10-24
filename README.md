# OLAP Dashboard

Short run and build instructions for the OLAP Dashboard React app.

## Prerequisites

- Node.js (recommended LTS, e.g. 16.x or 18.x)
- npm (bundled with Node.js)

You can check your versions with:

```powershell
node -v
npm -v
```

## Install dependencies

From the project root folder, run:

```powershell

npm install
```

This will create `node_modules` and install `react-scripts` and other dependencies.

## Run (development)

Start the dev server:

```powershell
npm start
```

By default the development server runs on port 3000. To change the port in PowerShell for the current session:

```powershell
#$env:PORT = 3001; npm start
```

## Build (production)

Create an optimized production build:

```powershell
npm run build
```

The built assets will be in the `build/` folder. To serve them locally for verification you can use `serve`:

```powershell
npx serve -s build
# or install globally: npm install -g serve ; serve -s build
```

## Install Python dependencies:
```powershell
pip install pandas numpy python-dotenv sqlalchemy pymysql mysql-connector-python openpyxl jupyter
```

# Change password for MySql Server Code
Make sure to change etl.ipynb on the section "3.Load" when running. I have added comments for you to run it.
You also have to change the index.js in the server folder inside the root project directory.

