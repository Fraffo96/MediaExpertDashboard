Loghi brand (PNG consigliato)
===============================
Un file per brand, nome file = brand_id numerico (come in Admin / dim_brand), es.:
  20.png  → Acer
  1.png   → Samsung

Percorso pubblico: /static/img/brands/<brand_id>.png

I PNG possono avere qualsiasi risoluzione: il CSS usa object-fit: contain dentro riquadri fissi (home + topbar).

Deploy: copia questi file nella cartella brands nell'immagine Docker (restano sotto app/static/img/brands/).
