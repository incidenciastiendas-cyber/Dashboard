const API_URL = "http://127.0.0.1:5000/incidencias";

async function cargarDatos() {
  const contResumen = document.getElementById("resumen");
  const thead = document.getElementById("thead");
  const tbody = document.getElementById("tbody");

  contResumen.innerHTML = "<p class='text-center text-muted'>Cargando datos...</p>";

  try {
    const resp = await fetch(API_URL);
    const data = await resp.json();

    contResumen.innerHTML = `
      <div class="col-md-3">
        <div class="card shadow-sm text-center p-3">
          <h5>Total registros</h5>
          <h2>${data.total.toLocaleString()}</h2>
        </div>
      </div>
    `;

    // Cabecera
    thead.innerHTML = "";
    const headers = data.columnas.slice(0, 10); // solo las primeras 10 para vista rápida
    headers.forEach(col => {
      const th = document.createElement("th");
      th.textContent = col;
      thead.appendChild(th);
    });

    // Cuerpo
    tbody.innerHTML = "";
    data.muestra.forEach(row => {
      const tr = document.createElement("tr");
      headers.forEach(h => {
        const td = document.createElement("td");
        td.textContent = row[h] || "";
        tr.appendChild(td);
      });
      tbody.appendChild(tr);
    });

  } catch (err) {
    console.error(err);
    contResumen.innerHTML = `<p class='text-danger text-center'>Error al cargar datos</p>`;
  }
}

document.addEventListener("DOMContentLoaded", cargarDatos);
