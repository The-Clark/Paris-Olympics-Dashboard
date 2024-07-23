document.addEventListener('DOMContentLoaded', function() {
    // Handle the search functionality
    const searchBar = document.getElementById('search-bar');
    if (searchBar) {
        searchBar.addEventListener('input', function() {
            const query = searchBar.value;
            const clientRef = document.getElementById('client-select').value;
            fetch(`/?q=${query}&client_ref=${clientRef}`)
                .then(response => response.text())
                .then(html => {
                    const parser = new DOMParser();
                    const doc = parser.parseFromString(html, 'text/html');
                    const projectsList = doc.getElementById('projects-list').innerHTML;
                    document.getElementById('projects-list').innerHTML = projectsList;
                });
        });
    }

    // Handle the client selection
    const clientSelect = document.getElementById('client-select');
    if (clientSelect) {
        clientSelect.addEventListener('change', function() {
            const clientRef = clientSelect.value;
            const query = searchBar.value;
            fetch(`/?q=${query}&client_ref=${clientRef}`)
                .then(response => response.text())
                .then(html => {
                    const parser = new DOMParser();
                    const doc = parser.parseFromString(html, 'text/html');
                    const projectsList = doc.getElementById('projects-list').innerHTML;
                    document.getElementById('projects-list').innerHTML = projectsList;
                });
        });
    }

    // Handle the filter form submission
    const form = document.getElementById('filter-form');
    if (form) {
        form.addEventListener('submit', function(event) {
            event.preventDefault();
            const clientRef = document.getElementById('client-select').value;
            window.location.href = `/client/${clientRef}`;
        });
    }
});
