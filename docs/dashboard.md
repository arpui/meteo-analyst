# Dashboard (dashboard_meteo.html)

Single-page app, pura HTML+JS. Consumeix Chart.js (CDN).

## Estructura

### Tabs d'estació
- **Torrelles** (active per defecte)
- **Espui**
- **Tot** — gràfics comparatius (Torrelles + Espui al mateix canvas)

Les pestanyes s'amaguen si s'usa `?estacio=torrelles` o `?estacio=espui` (mode estàtic per l'app Android).

### Cards (sensors última lectura)
| Card | Contingut | Condicional |
|------|-----------|-------------|
| Temperatura | valor, sensació, punt de rosada, min/max amb hora | CH1 si `temp_ch1 != null` |
| Humitat | valor, min/max amb hora | CH1 si `hum_ch1 != null` |
| Pressió | hPa rel, abs, VPD, min/max amb hora | — |
| Vent | brúixola (direcció + graus), velocitat, ratxa màxima del dia amb hora | — |
| Solar/UV | W/m², UV, lux | — |
| Pluja | rate, diària, horària; llamps (distància + count) | secció llamps només si `lightning_count != null` |
| **Sensor CH1** | temperatura i humitat CH1 | **només si `temp_ch1 != null` o `hum_ch1 != null`** |

Colors: valors secundaris en `#d4a574` (taronja pàl·lid). Mínims en blau `#5ba4ef` (↓), màxims en vermell `#e87070` (↑).

### Fotos
Strip horitzontal scrollable amb thumbnails (90×68px). Clica per ampliar:
- Navegació tàctil (swipe esquerra/dreta)
- Clic a esquerra/dreta de la foto
- Fletxes de teclat
- Escape per tancar

Subsampleig: una foto cada 30 minuts (parametritzable via `?interval=N`).

### Gràfics (Chart.js)
| Gràfic | Tipus | Condicional |
|--------|-------|-------------|
| Temperatura i Humitat | línia (dual eix Y) | sempre |
| Pressió | línia | sempre |
| Vent + Ratxes | línia (separades, ratxa discontínua) | sempre |
| Pluja | barres | només si alguna lectura `rain_rate > 0` |
| Comparativa sensors (CH1) | línia (4 datasets, dual eix Y) | només si hi ha dades CH1 |
| Llamps | línia + barra combinats | només si algun `lightning_count > 0` |

### Refresc automàtic
Cada 60 segons via `setInterval`. Botó ↻ per refresc manual.

## Detecció de path

El dashboard detecta automàticament si és accés local o remot:

```javascript
// Local:  http://192.168.31.225:8765/dashboard        → API = ''
// Remot:  https://xxx.duckdns.org/temps/dashboard      → API = '/temps'
const API = window.location.pathname.includes('/dashboard')
  ? path.substr(0, path.lastIndexOf('/dashboard'))
  : '';
```

## Temes

- Fons: `#0d1117` (dark GitHub-ish)
- Targetes: `#161b22` amb bordes `#2a2a3a`
- Text principal: `#e0e0e0`
- Text secundari: `#555` / `#666`
- Colors charts: vermell `#e87070` (temp), blau `#5ba4ef` (humitat/pluja), porpra `#a78bfa` (pressió), verd `#5dcaa5` (vent), taronja pàl·lid `#d4a574` (ratxes/valors secundaris)
