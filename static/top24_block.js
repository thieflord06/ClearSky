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
document.querySelector('a[href="/funer_facts"]').addEventListener('click', function (event) {
    // Prevent the link from navigating immediately
    event.preventDefault();

    // Show the loading screen
    showLoadingScreen();

    // Simulate an asynchronous operation (e.g., fetching data)
    setTimeout(function () {
        // After the operation is complete, hide the loading screen
        hideLoadingScreen();

        // Navigate to the "fun facts" page
        window.location.href = '/funer_facts';
    }, 2000); // Adjust the delay as needed for your use case
});