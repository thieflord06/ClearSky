document.addEventListener('DOMContentLoaded', function() {
    const selectionForm = document.getElementById('search-form');
    const loadingScreen = document.getElementById('loading-screen');
    const resultContainer = document.getElementById('result-container');
    const resultText = document.getElementById('result-text');
    const indexContainer = document.getElementById('index-container');
    const formContainer = document.getElementById('form-container');
    const baseContainer = document.getElementById('base-container');
    const blockListContainer = document.getElementById('blocklist-container');
    const comingSoonContainer = document.getElementById('comingsoon-container');
    const errorContainer = document.getElementById('error-container');
    const pendingRequestContainer = document.getElementById('pending-request-container');
    const timeoutContainer = document.getElementById('timeout-container');
    const TIMEOUT_DURATION = 180000;

    let requestInProgress = false;
    let optionSelected;
    const submitButton = document.getElementById('submit-button');

    function handleTimeout() {
        // Perform actions when the server doesn't respond within the specified timeout
        // For example, you can display an error message or take other appropriate actions.
        console.log('Server request timed out. Please try again later.');
        hideInProgressContainer(); // Hide the "request in progress" container if it was shown
        showTimeoutContainer(); // Show the error container with a timeout message
    }

    // Function to show the loading screen
    function showLoadingScreen() {
        console.log('showLoadingScreen() called');
        loadingScreen.style.display = 'block';
    }

    function showComingSoonContainer() {
        console.log('showLoadingScreen() called');
        comingSoonContainer.style.display = 'block';
    }

    function hideComingSoonContainer() {
        console.log('hideLoadingScreen() called');
        comingSoonContainer.style.display = 'none';
    }

    // Function to hide the loading screen
    function hideLoadingScreen() {
        console.log('hideLoadingScreen() called');
        loadingScreen.style.display = 'none';
    }

    function showErrorContainer() {
        console.log('showErrorContainer() called');
        errorContainer.style.display = 'block';
    }

    function hideErrorContainer() {
        console.log('hideErrorContainer() called');
        errorContainer.style.display = 'none';
    }

    function showInProgressContainer() {
        console.log('showInProgressContainer() called');
        pendingRequestContainer.style.display = 'block'
    }

    function hideInProgressContainer() {
        console.log('hideInProgressContainer() called');
        pendingRequestContainer.style.display = 'none'
    }

    // Function to show the result container
    function showResultContainer() {
        console.log('showResultContainer() called');
        resultContainer.style.display = 'block';
    }

    // Function to hide the result container
    function hideResultContainer() {
        console.log('hideResultContainer() called');
        resultContainer.style.display = 'none';
    }

    function showBlockListContainer() {
        console.log('showBlockListContainer() called');
        blockListContainer.style.display = 'block';
    }

    function hideBaseContainer() {
        console.log('hideBaseContainer() called.');
        baseContainer.style.display = 'none';
    }

    function showBaseContainer() {
        console.log('showBaseContainer() called.');
        baseContainer.style.display = 'block';
    }

    function hideBlockListContainer() {
        console.log('hideBlockListContainer() called');
        blockListContainer.style.display = 'none';
    }

    function hideIndexContainer() {
        console.log('hideIndexContainer() called');
        indexContainer.style.display = 'none';
    }

    function showIndexContainer() {
        console.log('showIndexContainer() called.');
        indexContainer.style.display = 'block';
    }

    // Function to display the result data
    function showResult(data) {
        console.log('showResult() called');
        resultText.innerHTML = ''; // Clear the previous result text

        if (data.result) {
            const resultParagraph = document.createElement('p');
            resultParagraph.textContent = data.result;
            resultText.appendChild(resultParagraph);
            hideLoadingScreen();
            showResultContainer();
        }
        else if (data.block_list) {
            if (Array.isArray(data.block_list)) {
                const blockListData = document.getElementById('block-list-data');
                const userHeading = document.getElementById('user-heading');
                const blockCount = document.getElementById('block-count');
                const fragment = document.createDocumentFragment();

                userHeading.textContent = 'For User: ' + data.user;
                blockCount.textContent = `Total Blocked Users: ${data.count}`;

                blockListData.innerHTML = '';

                data.block_list.forEach(item => {
                    const timestamp = new Date(item.timestamp);
                    const formattedDate = timestamp.toLocaleDateString('en-US', {timeZone: 'UTC'}); // Format date
                    const blockItem = document.createElement('li');

                    blockItem.textContent = `Handle: ${item.handle}, Date: ${formattedDate}`;
                    blockListData.appendChild(blockItem);

                    hideLoadingScreen();
                    showBlockListContainer();
                });
            }
            else {
                const noResultParagraph = document.createElement('p');
                noResultParagraph.textContent = 'No result found.';
                resultText.appendChild(noResultParagraph);
                hideLoadingScreen();
                showResultContainer(); // Show result container
            }
        }
        else if (data.count) {
            // Display result container with total count
//                const countParagraph = document.createElement('p');
            resultText.textContent = `Total User count: ${data.count}`;
//                resultText.appendChild(countParagraph);
            hideLoadingScreen();
            showResultContainer();
        }
        else if (data.who_block_list) {
            const blockListData = document.getElementById('block-list-data');
            const userHeading = document.getElementById('user-heading');
            const blockCount = document.getElementById('block-count');
            const fragment = document.createDocumentFragment();

            userHeading.textContent = 'For User: ' + data.user;
            blockCount.textContent = `Total Users that block this account: ${data.counts}`;

            blockListData.innerHTML = '';

        data.who_block_list.forEach((item, index) => {
            const timestamp = new Date(data.date[index]);
            const formattedDate = timestamp.toLocaleDateString('en-US', { timeZone: 'UTC' });
            const blockItem = document.createElement('li');
//            console.log(data.who_block_list);
//            console.log(item)

            blockItem.innerHTML = `Handle: ${item}, Date: ${formattedDate}`;
            blockListData.appendChild(blockItem);
        });

                hideLoadingScreen();
                showBlockListContainer();
        }
        else {
            const noResultParagraph = document.createElement('p');
            noResultParagraph.textContent = 'No result found.';
            resultText.appendChild(noResultParagraph);
            hideLoadingScreen();
            showResultContainer(); // Show result container
        }
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

        optionSelected = document.getElementById("selection").value;

        // Mark that a request is in progress
        requestInProgress = false;

        if (selection.value === '5') {
            // Create a new FormData object and add a flag to indicate "Option 5" should not be processed
            const formData = new FormData(selectionForm);
            formData.append('skipOption5', 'true');

            // If Option 5 is selected, redirect to the "Coming Soon" page
            fetch('/selection_handle', {
                method: 'POST',
                body: formData
//                body: new FormData(selectionForm)
            })
            .then(response => {
                // Check if the response status is successful (HTTP 200-299)
                if (!response.ok) {
                    throw new Error('Network response was not ok');
                }
                hideBaseContainer();
                hideIndexContainer();
                showComingSoonContainer();
                return; // Return to prevent further execution
            })
            .catch(error => {
                // Handle any errors here
                hideBaseContainer();
                hideIndexContainer();
                showErrorContainer();
                console.error('Error submitting form:', error);
                // You can show an error message to the user if needed
            });
            return; // Return to prevent further execution
        }
        if (optionSelected === "3") {
//            var confirmed = window.confirm("This will take an extremely long time! Do you want to proceed?");
            alert("Getting results may take some time, if the user is blocking a lot of accounts.");
//            if (!confirmed) {
//                return;
//            }
        }
        if (optionSelected === "5") {
            alert("Getting results may take some time, if the user is blocked by a lot of accounts.");
        }
        if (requestInProgress) {
            // A request is already in progress, do not make another request
            hideIndexContainer();
            hideIndexContainer();
            showInProgressContainer();
            return;
        }

        // Mark that a request is in progress
        requestInProgress = false;

        submitButton.disabled = true; // Disable the form submission button
        hideBaseContainer();
        hideIndexContainer();
        showLoadingScreen(); // Show the loading screen

        // Set up the timeout
        const timeoutId = setTimeout(() => {
            // Function to execute if the timeout occurs before the server responds
            handleTimeout(); // You need to implement this function (see Step 3)
        }, TIMEOUT_DURATION);

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

            // Reset the requestInProgress flag to allow future requests
            requestInProgress = false;
            submitButton.disabled = false; // Re-enable the form submission button
        })
        .catch(error => {
            // Handle any errors here
            handleErrors(error); // Call the function to handle errors and show the index container
            hideBaseContainer();
            hideIndexContainer();
            showErrorContainer();
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