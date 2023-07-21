//const selectionForm = document.getElementById('search-form');
//const loadingScreen = document.getElementById('loading-screen');
//const resultContainer = document.getElementById('result-container');
//const resultText = document.getElementById('result-text');
//const indexContainer = document.getElementById('index-container');
//const selectionForm = document.getElementById('search-form');
//const loadingScreen = document.getElementById('loading-container');
//const resultContainer = document.getElementById('result-container');
//const blockListContainer = document.getElementById('block-list-container');
//const resultText = document.getElementById('result-text');
//const blockList = document.getElementById('block-list');
//const indexContainer = document.getElementById('index-container');
//const formContainer = document.getElementById('form-container');
document.addEventListener('DOMContentLoaded', function() {
    const selectionForm = document.getElementById('search-form');
    const loadingScreen = document.getElementById('loading-screen');
    const resultContainer = document.getElementById('result-container');
    const resultText = document.getElementById('result-text');
    const indexContainer = document.getElementById('index-container');
    const formContainer = document.getElementById('form-container');

    // Function to show the loading screen
    function showLoadingScreen() {
        loadingScreen.style.display = 'block';
        indexContainer.style.display = 'none';
    }

    // Function to hide the loading screen
    function hideLoadingScreen() {
        loadingScreen.style.display = 'none';
        indexContainer.style.display = 'block';
    }

    // Function to show the result container
    function showResultContainer() {
        resultContainer.style.display = 'block';
        indexContainer.style.display = 'none';
    }

    // Function to hide the result container
    function hideResultContainer() {
        resultContainer.style.display = 'none';
        indexContainer.style.display = 'block';
    }

    // Function to display the result data
    function showResult(data) {
        resultText.innerHTML = ''; // Clear the previous result text

        if (data.result) {
            const resultParagraph = document.createElement('p');
            resultParagraph.textContent = data.result;
            resultText.appendChild(resultParagraph);
        } else if (data.block_list) {
            const blockListDiv = document.createElement('div');
            data.block_list.forEach(item => {
                const blockItem = document.createElement('p');
                blockItem.textContent = `Handle: ${item.handle}, Date: ${item.timestamp}`;
                blockListDiv.appendChild(blockItem);
            });
            resultText.appendChild(blockListDiv);
        } else {
            const noResultParagraph = document.createElement('p');
            noResultParagraph.textContent = 'No result found.';
            resultText.appendChild(noResultParagraph);
        }

        // Show the result container
        showResultContainer();
    }

    // Function to handle errors and show the index container if needed
    function handleErrors(error) {
        console.error("Error fetching data:", error);
        hideLoadingScreen(); // Hide the loading screen in case of an error
        hideResultContainer(); // Hide the result container and show the index container
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
            // Return the response JSON data
            return response.json();
        })
        .then(data => {
            // Update the resultText container with the server response
            showResult(data);
            // Hide the loading screen
            hideLoadingScreen();
            indexContainer.style.display = 'none';

        })
        .catch(error => {
            // Handle any errors here
            handleErrors(error); // Call the function to handle errors and show the index container
        });
    });

    // Add event listener to the selection dropdown
    selection.addEventListener('change', function() {
        if (selection.value === '4') {
            // If "Get Total Users Count" is selected, disable the identifier field
            identifier.value = '';
            identifier.readOnly = true;
        } else {
            // Enable the identifier field
            identifier.readOnly = false;
        }
    });
});