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

## Common issue: 'react-scripts' is not recognized

If you see an error like:

```
'react-scripts' is not recognized as an internal or external command,
operable program or batch file.
```

It usually means dependencies were not installed or `node_modules` is missing. Fixes:

1. Run `npm install` from the project root.
2. Do NOT install `react-scripts` globally. It should remain a local dependency.
3. If problems persist, try deleting `node_modules` and `package-lock.json`, then reinstall:

```powershell
rm -Recurse -Force node_modules
del package-lock.json
npm install
```

Or use PowerShell's aliases if `rm`/`del` aren't available on your shell.

You can also run the local binary via `npx` which will prefer the project's installed copy:

```powershell
npx react-scripts build
```




