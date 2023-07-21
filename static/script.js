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
function showBlockList(data) {
    resultText.innerHTML = ''; // Clear the previous result text

    // Create a new div to hold the block list items
    const blockListDiv = document.createElement('div');

    data.forEach(item => {
        const blockItem = document.createElement('p');
        // Extract the date part from the timestamp (index 0 to 10)
        const datePart = item.timestamp.slice(4, 12);
        blockItem.textContent = `Handle: ${item.handle}, Date: ${datePart}`;
        blockListDiv.appendChild(blockItem);
    });

    // Append the block list container to the result text
    resultText.appendChild(blockListDiv);

    // Show the result container
    resultContainer.style.display = 'block';
}

// Function to display the total user count
function showTotalUsersCount(data) {
    // Clear the previous result text
    resultText.innerHTML = '';

    const resultParagraph = document.createElement('p');
    resultParagraph.textContent = `Total Users Count: ${data.count}`;

    // Append the result paragraph to the result text
    resultText.appendChild(resultParagraph);

    // Show the result container
    resultContainer.style.display = 'block';
}


// Function to display the result data
function showResult(data) {
    // Check if the selection is for "Get Total Users Count" (option 4)
    if (selection.value === '4') {
        // For the "Total Users Count" option, show the result in total_users.html
        showTotalUsersCount(data);
    } else {
        // For other selections (options 1, 2, 3, 5), update the result text as before
        resultText.innerHTML = ''; // Clear the previous result text

        const resultContent = document.createElement('div');

        if (data.result) {
            const resultParagraph = document.createElement('p');
            resultParagraph.textContent = data.result;
            resultContent.appendChild(resultParagraph);
        } else {
            const noResultParagraph = document.createElement('p');
            noResultParagraph.textContent = 'No result found.';
            resultContent.appendChild(noResultParagraph);
        }

        // Append the result content to the result text
        resultText.appendChild(resultContent);

        // Show the result container
        resultContainer.style.display = 'block';
    }
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

        // Call the showResult function to display the data based on selection value
        showResult(data);

        // Show the result container
        resultContainer.style.display = 'block';
    })
    .catch(error => {
        // Handle any errors here
        console.error("Error fetching data:", error);
        hideLoadingScreen(); // Hide the loading screen in case of an error
    });
});
