// loading.js

// Function to show the loading screen
function showLoadingScreen() {
    document.getElementById('loading-screen').style.display = 'block';
}

// Function to hide the loading screen
function hideLoadingScreen() {
    document.getElementById('loading-screen').style.display = 'none';
}

// Attach an event listener to the "Block Statistics" link
document.querySelector('a[href="/block_stats"]').addEventListener('click', function (event) {
    // Prevent the link from navigating immediately
    event.preventDefault();

    // Show the loading screen
    showLoadingScreen();

    // Simulate an asynchronous operation (e.g., fetching data from the server)
    fetch('/block_stats') // Make a request to your server
        .then(response => {
            // Hide the loading screen when data is received
            hideLoadingScreen();

            // Check if the response status is OK (200)
            if (response.ok) {
                // Navigate to the "Block Statistics" page
                window.location.href = '/block_stats';
            } else {
                // Handle the case where the request failed
                console.error('Failed to fetch data');
            }
        })
        .catch(error => {
            // Handle network errors
            console.error('Network error:', error);
            // Hide the loading screen in case of an error
            hideLoadingScreen();
        });
});
