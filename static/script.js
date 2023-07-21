const selectionForm = document.getElementById('search-form');
const loadingScreen = document.getElementById('loading-screen');
const resultContainer = document.getElementById('result-container');
const blockListContainer = document.getElementById('block-list-container');
const resultText = document.getElementById('result-text');
const blockList = document.getElementById('block-list');

// Function to show the loading screen
function showLoadingScreen() {
    loadingScreen.style.display = 'block';
}

// Function to hide the loading screen
function hideLoadingScreen() {
    loadingScreen.style.display = 'none';
}

// Function to show the result container
function showResultContainer() {
    resultContainer.style.display = 'block';
}

// Function to show the block list container
function showBlockListContainer() {
    blockListContainer.style.display = 'block';
}

// Function to hide the result container
function hideResultContainer() {
    resultContainer.style.display = 'none';
}

// Function to hide the block list container
function hideBlockListContainer() {
    blockListContainer.style.display = 'none';
}

// Function to display the blocklist data
// Function to display the blocklist data
function showBlockList(data) {
    resultText.innerHTML = ''; // Clear the previous result text

    // Create a new div to hold the block list items
    const blockListContainer = document.createElement('div');

    data.forEach(item => {
        const blockItem = document.createElement('p');
        // Extract the date part from the timestamp (index 0 to 10)
        const datePart = item.timestamp.slice(4, 12);
        blockItem.textContent = `Handle: ${item.handle}, Date: ${datePart}`;
        blockListContainer.appendChild(blockItem);
    });

    // Append the block list container to the result text
    resultText.appendChild(blockListContainer);

    // Show the result container
    resultContainer.style.display = 'block';
}


// Add event listener to the form submit button
selectionForm.addEventListener('submit', function (event) {
    event.preventDefault(); // Prevent the default form submission
    showLoadingScreen(); // Show the loading screen

    // Perform your form submission or AJAX request using JavaScript Fetch API or Axios
    fetch('/selection_handle', {
        method: 'POST',
        body: new FormData(selectionForm)
    })
    .then(response => {
        // Check if the response status is successful (HTTP 200-299)
        if (!response.ok) {
            throw new Error('Network response was not ok');
        }
        // Return the response data directly (it's already parsed JSON)
        return response.json();
    })
    .then(data => {
        // Process the JSON data here
        hideLoadingScreen(); // Hide the loading screen once the results are ready

        // Check if the selection is for "Get Block List of a User" (option 3)
        if (selection.value === '3') {
            // Call the showBlockList function to display the blocklist data
            showBlockList(data.block_list);
        } else {
            // For other selections (options 1, 2, 4, 5), update the result text as before
            resultText.textContent = data.result;
        }

        // Show the result container
        resultContainer.style.display = 'block';
    })
    .catch(error => {
        // Handle any errors here
        console.error("Error fetching data:", error);
        hideLoadingScreen(); // Hide the loading screen in case of an error
    });
});
