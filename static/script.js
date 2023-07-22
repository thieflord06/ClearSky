document.addEventListener('DOMContentLoaded', function() {
    const selectionForm = document.getElementById('search-form');
    const loadingScreen = document.getElementById('loading-screen');
    const resultContainer = document.getElementById('result-container');
    const resultText = document.getElementById('result-text');
    const indexContainer = document.getElementById('index-container');
    const formContainer = document.getElementById('form-container');
    const baseContainer = document.getElementById('base-container')
    const blockListContainer = document.getElementById('block-list-container')

    let requestInProgress = false;
    const submitButton = document.getElementById('submit-button');

    // Function to show the loading screen
    function showLoadingScreen() {
        console.log('showLoadingScreen() called');
        loadingScreen.style.display = 'block';
        indexContainer.style.display = 'none';
    }

    // Function to hide the loading screen
    function hideLoadingScreen() {
        console.log('hideLoadingScreen() called');
        loadingScreen.style.display = 'none';
        indexContainer.style.display = 'block';
    }

    // Function to show the result container
    function showResultContainer() {
        console.log('showResultContainer() called');
        resultContainer.style.display = 'block';
        indexContainer.style.display = 'none';
    }

    // Function to hide the result container
    function hideResultContainer() {
        console.log('hideResultContainer() called');
        resultContainer.style.display = 'none';
        indexContainer.style.display = 'block';
    }

    function showBlockListContainer() {
        console.log('showBlockListContainer() called');
        blockListContainer.style.display = 'block';
        indexContainer.style.display = 'none'; //
    }

    function hideBlockListContainer() {
        console.log('hideBlockListContainer() called');
        blockListContainer.style.display = 'none';
        indexContainer.style.display = 'block'; //
    }

    // Function to display the result data
    function showResult(data) {
        console.log('showResult() called');
        resultText.innerHTML = ''; // Clear the previous result text

        if (data.result) {
            const resultParagraph = document.createElement('p');
            resultParagraph.textContent = data.result;
            resultText.appendChild(resultParagraph);
        } else if (data.block_list) {
            if (Array.isArray(data.block_list)) {
                // Option 3: Block List of a specific user
                const blockListDiv = document.createElement('div');
                const userHeading = document.createElement('h2');
                const blockCount = document.createElement('h3');
                blockCount.textContent = `Count: ${data.count}`;
                userHeading.textContent = `Block List: ${data.user}`;

                blockListDiv.appendChild(userHeading);

                data.block_list.forEach(item => {
                    const blockItem = document.createElement('p');
                    const timestamp = new Date(item.timestamp);
                    const formattedDate = timestamp.toLocaleDateString(); // Format date as per locale
                    blockItem.textContent = `Handle: ${item.handle}, Date: ${formattedDate}`;
                    blockListDiv.appendChild(blockItem);
                });
                resultText.appendChild(blockListDiv);
                showBlockListContainer();
            }
        } else if (typeof data.count === 'number') {
            resultParagraph.textContent = data.result;
            resultText.appendChild(resultParagraph);
            showResultContainer(); // Show result container
        } else if (typeof data.count === 'number') {
            // Display result container with total count
            const countParagraph = document.createElement('p');
            countParagraph.textContent = `Total User count: ${data.count}`;
            resultText.appendChild(countParagraph);
            showResultContainer();
        } else {
            const noResultParagraph = document.createElement('p');
            noResultParagraph.textContent = 'No result found.';
            resultText.appendChild(noResultParagraph);
            showResultContainer(); // Show result container
        }

        // Show the result container
        showResultContainer();
        showBlockListContainer();
    }

    // Function to handle errors and show the index container if needed
    function handleErrors(error) {
        console.error("Error fetching data:", error);

        const errorParagraph = document.createElement('p');
        errorParagraph.textContent = 'Error fetching data. Please try again later.';
        resultText.appendChild(errorParagraph);

        hideLoadingScreen(); // Hide the loading screen in case of an error
        hideResultContainer(); // Hide the result container and show the index container
        hideBlockListContainer();
    }

    // Add event listener to the form submit button
    selectionForm.addEventListener('submit', function (event) {
        event.preventDefault(); // Prevent the default form submission
        console.log('Form submit button clicked');

        if (requestInProgress) {
            // A request is already in progress, do not make another request
            return;
        }

        // Mark that a request is in progress
        requestInProgress = true;

        showLoadingScreen(); // Show the loading screen
        submitButton.disabled = true; // Disable the form submission button

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
            baseContainer.style.display = 'none';

            // Reset the requestInProgress flag to allow future requests
            requestInProgress = false;
            submitButton.disabled = false; // Re-enable the form submission button
        })
        .catch(error => {
            // Handle any errors here
            handleErrors(error); // Call the function to handle errors and show the index container

        // Reset the requestInProgress flag in case of an error
        requestInProgress = false;
        submitButton.disabled = false;
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