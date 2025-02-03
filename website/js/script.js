const darkModeBtn = document.getElementById('darkModeBtn');
darkModeBtn.addEventListener('click', () => {
    document.body.classList.toggle('dark-mode');
    const mode = document.body.classList.contains('dark-mode') ? '🌕' : '🌙';
    darkModeBtn.textContent = mode;
});

const searchInput = document.getElementById('searchInput');
const filterBtn = document.getElementById('filterBtn');

filterBtn.addEventListener('click', () => {
    const query = searchInput.value.toLowerCase();
    const rows = document.querySelectorAll('tbody tr');
    
    rows.forEach(row => {
        const trailerName = row.querySelector('td:first-child').textContent.toLowerCase();
        if (trailerName.includes(query)) {
            row.style.display = '';
        } else {
            row.style.display = 'none';
        }
    });
});

function showLoading() {
    document.getElementById('loadingScreen').style.display = 'flex';
}

function loadData() {
    showLoading();
    setTimeout(function() {
        document.getElementById('loadingScreen').style.display = 'none';
    }, 3000); 
}

window.onload = function() {
    loadData(); 
};
