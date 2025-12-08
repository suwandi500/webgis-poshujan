const USERNAME = "gaw97094";
const PASSWORD = "97094";
let isLoggedIn = false;

const kecamatanData = {
  Palu: ["Palu Barat", "Palu Timur"],
  Sigi: ["Dolo", "Sigi Biromaru"],
  Parigi: ["Parigi", "Ampibabo"],
};

const markersData = [
  { name: "Pos Hujan Bariri", lat: -1.39, lng: 119.9, kabupaten: "Sigi", kecamatan: "Dolo" },
  { name: "Pos Hujan Sigi", lat: -1.45, lng: 120.1, kabupaten: "Sigi", kecamatan: "Sigi Biromaru" },
  { name: "Pos Hujan Palu Timur", lat: -0.9, lng: 119.8, kabupaten: "Palu", kecamatan: "Palu Timur" },
];

let map;
let activeMarkers = [];

function filterMarkers() {
  activeMarkers.forEach((marker) => map.removeLayer(marker));
  activeMarkers = [];

  const selectedKab = document.getElementById("filterKabupaten").value;
  const selectedKec = document.getElementById("filterKecamatan").value;

  let bounds = [];

  markersData.forEach((pos) => {
    const matchKab = !selectedKab || pos.kabupaten === selectedKab;
    const matchKec = !selectedKec || pos.kecamatan === selectedKec;
    if (matchKab && matchKec) {
      const marker = L.marker([pos.lat, pos.lng])
        .addTo(map)
        .bindPopup(
          `<strong>${pos.name}</strong><br>Kabupaten: ${pos.kabupaten}<br>Kecamatan: ${pos.kecamatan}`
        );
      activeMarkers.push(marker);
      bounds.push([pos.lat, pos.lng]);
    }
  });

  if (bounds.length > 0) {
    map.fitBounds(bounds, { padding: [50, 50] });
  }
}

function updateUIBasedOnLogin() {
  document.getElementById("loginBtn").style.display = isLoggedIn ? "none" : "inline-block";
  document.getElementById("profileArea").style.display = isLoggedIn ? "inline-block" : "none";
  document.getElementById("menuPosHujan").style.display = isLoggedIn ? "inline-block" : "none";
}

function handleLogin() {
  const u = document.getElementById("username").value;
  const p = document.getElementById("password").value;
  if (u === USERNAME && p === PASSWORD) {
    isLoggedIn = true;
    localStorage.setItem("isLoggedIn", "true");
    updateUIBasedOnLogin();
    hidePopups();
    showToast("Login berhasil!");
  } else {
    showToast("Username atau password salah.");
  }
}

function handleLogout() {
  isLoggedIn = false;
  localStorage.removeItem("isLoggedIn");
  updateUIBasedOnLogin();
  hidePopups();
  showToast("Berhasil logout.");
}

function hidePopups() {
  const loginBox = document.getElementById("loginBox");
  if (loginBox) loginBox.style.display = "none";
}

function showToast(msg) {
  const toast = document.getElementById("toast");
  if (!toast) return;
  toast.textContent = msg;
  toast.style.display = "block";
  setTimeout(() => (toast.style.display = "none"), 3000);
}

// Biar bisa dipanggil dari HTML (onclick/onsubmit)
window.handleLogin = handleLogin;
window.handleLogout = handleLogout;
window.hidePopups = hidePopups;

// Init setelah DOM siap
document.addEventListener("DOMContentLoaded", () => {
  // Buat peta
  map = L.map("map").setView([-1.5, 120.0], 7);

  // Layer OSM
  L.tileLayer("https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png", {
    attribution:
      '&copy; <a href="https://www.openstreetmap.org/copyright">OpenStreetMap</a> contributors',
    subdomains: ["a", "b", "c"],
    maxZoom: 19,
  }).addTo(map);

  // Boundary provinsi
  fetch("https://raw.githubusercontent.com/superpikar/indonesia-geojson/master/indonesia-prov.geojson")
    .then((res) => res.json())
    .then((data) => {
      L.geoJSON(data, {
        style: {
          color: "#444",
          weight: 1,
          fillOpacity: 0,
        },
        onEachFeature: function (feature, layer) {
          if (feature.properties && feature.properties.Propinsi) {
            layer.bindTooltip(feature.properties.Propinsi, { permanent: false });
          }
        },
      }).addTo(map);
    });

  // Login state
  isLoggedIn = localStorage.getItem("isLoggedIn") === "true";
  updateUIBasedOnLogin();

  // Event listeners
  document.getElementById("loginBtn").addEventListener("click", () => {
    hidePopups();
    document.getElementById("loginBox").style.display = "block";
  });

  document.getElementById("filterKabupaten").addEventListener("change", function () {
    const kab = this.value;
    const kecSelect = document.getElementById("filterKecamatan");
    kecSelect.innerHTML = '<option value="">Kecamatan</option>';
    if (kecamatanData[kab]) {
      kecamatanData[kab].forEach((kec) => {
        const opt = document.createElement("option");
        opt.value = kec;
        opt.textContent = kec;
        kecSelect.appendChild(opt);
      });
    }
    filterMarkers();
  });

  document
    .getElementById("filterKecamatan")
    .addEventListener("change", filterMarkers);

  document.getElementById("resetFilterBtn").addEventListener("click", () => {
    document.getElementById("filterKabupaten").value = "";
    document.getElementById("filterKecamatan").innerHTML =
      '<option value="">Kecamatan</option>';
    filterMarkers();
  });

  document.addEventListener("click", function (event) {
    const loginBox = document.getElementById("loginBox");
    if (!loginBox) return;
    if (!loginBox.contains(event.target) && event.target.id !== "loginBtn") {
      hidePopups();
    }
  });

  // Tampilkan marker awal
  filterMarkers();
});
