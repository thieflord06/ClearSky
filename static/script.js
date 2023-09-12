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
    const submitButton = document.getElementById('submit-button');
    const identifierInput = document.getElementById('identifier');
    const TIMEOUT_DURATION = 180000;
    const handleInput = document.getElementById('identifier');
    const autocompleteSuggestions = document.getElementById('autocomplete-suggestions');

//    // Example: Push a new state with the state object
//    function pushNewState() {
//        history.pushState({ fromBackButton: true }, 'Home', '/');
//    }

//    // Handle the back button behavior
//    window.addEventListener('popstate', function(event) {
//        if (event.state && event.state.fromBackButton) {
//            console.log("inside");
//            window.location.href = '/';
//        }
//    });

//    pushNewState();

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
        console.log('hideBaseContainer() called');
        baseContainer.style.display = 'none';
    }

    function showBaseContainer() {
        console.log('showBaseContainer() called');
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
        console.log('showIndexContainer() called');
        indexContainer.style.display = 'block';
    }

    // Add event listener to the identifier input field
    identifierInput.addEventListener('input', function (event) {
        // Check if the input field is empty and the selected option is not 4
        const optionSelected = document.getElementById("selection").value;
        if (this.value.trim() === '' && optionSelected !== '4') {
            submitButton.disabled = true; // Disable the submit button
        } else {
            submitButton.disabled = false; // Enable the submit button
        }
    });

//    handleInput.addEventListener('input', function () {
//        const inputText = handleInput.value;
//
//        if (inputText === '') {
//        // If the input is empty, clear the suggestions and return
//        autocompleteSuggestions.innerHTML = '';
//        autocompleteSuggestions.style.display = 'none';
//
//        } else {
//        // Make an AJAX request to the server to fetch autocomplete suggestions
//        // Replace 'your_server_endpoint' with the actual endpoint on your server.
//        fetch(`/autocomplete?query=${inputText}`)
//            .then((response) => response.json())
//            .then((data) => {
//                // Clear previous suggestions
//                autocompleteSuggestions.innerHTML = '';
//                                // Show the suggestions div
//                autocompleteSuggestions.style.display = 'none';
//                // Display new suggestions
//                data.suggestions.forEach((suggestion) => {
//                    const suggestionItem = document.createElement('div');
//                    suggestionItem.textContent = suggestion;
//                    autocompleteSuggestions.appendChild(suggestionItem);
//
//                    // Attach a click event to each suggestion to fill the input field
//                    suggestionItem.addEventListener('click', () => {
//                        handleInput.value = suggestion;
//                        autocompleteSuggestions.innerHTML = ''; // Clear suggestions
//                    });
//                });
//                // Show the suggestions div
//                autocompleteSuggestions.style.display = 'block';
//            })
//            .catch((error) => {
//                console.error('Error fetching autocomplete suggestions:', error);
//            });
//        }
//    });

    // Add event listener to the form submit button
    selectionForm.addEventListener('submit', function (event) {
        // Check if the input field (identifier) is empty and set its value to "blank"
        if (identifierInput.value.trim() === '') {
            identifierInput.value = '';
        }

        hideIndexContainer();
        showLoadingScreen();
        submitButton.disabled = true; // Disable the form submission button
    });

    // Add event listener to the selection dropdown
    selection.addEventListener('change', function() {
        if (selection.value === '4') {
            // If "Get Total Users Count" is selected, disable the identifier field
            identifier.value = '';
            identifier.readOnly = true;
            submitButton.disabled = false;
        } else {
            // Enable the identifier field
            identifier.readOnly = false;
        }
    });
});