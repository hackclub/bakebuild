document.addEventListener("DOMContentLoaded", () => {
    const form = document.getElementById("notifyForm");
    const emailInput = document.getElementById("email");
    const feedbackMessage = document.getElementById("feedbackMessage");
    const submitButton = form.querySelector("button[type='submit']");

    form.addEventListener("submit", async (event) => {
        event.preventDefault();

        const email = emailInput.value.trim();

        if (!email) {
            showFeedback("Please enter a valid email.", "error");
            return;
        }

        disableButtonWithSpinner(submitButton, true);

        try {
            const response = await fetch("/api/submit-email", {
                method: "POST",
                headers: {
                    "Content-Type": "application/json",
                },
                body: JSON.stringify({ email }),
            });

            const data = await response.json();

            if (response.ok) {
                if (data.exists) {
                    showFeedback("Email already exists in the database.", "error");
                } else {
                    showFeedback("Thank you! You've been added to the list.", "success");
                    emailInput.value = "";
                }
            } else {
                throw new Error(data.message || "An error occurred. Please try again.");
            }
        } catch (error) {
            console.error("Error:", error);
            showFeedback("Something went wrong. Please try again later.", "error");
        } finally {
            disableButtonWithSpinner(submitButton, false);
        }
    });

    function showFeedback(message, type) {
        feedbackMessage.textContent = message;
        feedbackMessage.className = `feedback-message ${type}`;
        feedbackMessage.style.display = "block";

        setTimeout(() => {
            feedbackMessage.style.display = "none";
        }, 5000);
    }

    function disableButtonWithSpinner(button, disable) {
        if (disable) {
            button.disabled = true;
            button.innerHTML = `<span class="spinner"></span> RSVP`;
        } else {
            button.disabled = false;
            button.textContent = "RSVP";
        }
    }

    const shareButton = document.getElementById("shareButton");
    const fallbackContainer = document.getElementById("fallbackShare");

    const shareTitle = "BakeBuild - Design Your Own Cookie Cutter";
    const shareText = "Teens! Get creative with BakeBuild and design your own cookie cutter! 🚀🍪";
    const shareURL = "https://bakebuild.hackclub.com/";

    if (navigator.share) {
        shareButton.addEventListener("click", () => {
            navigator.share({
                title: shareTitle,
                text: shareText,
                url: shareURL
            }).catch((error) => console.error("Error sharing:", error));
        });
    } else {

        shareButton.style.display = "none";
        fallbackContainer.style.display = "block";

        const copyButton = document.getElementById("copyLinkButton");
        const copyMessage = document.getElementById("copyMessage");

        copyButton.addEventListener("click", () => {
            navigator.clipboard.writeText(shareURL)
                .then(() => {
                    copyMessage.textContent = "🔗 Link copied!";
                    copyMessage.style.opacity = "1";
                    setTimeout(() => {
                        copyMessage.style.opacity = "0";
                    }, 3000);
                })
                .catch((err) => console.error("Error copying link:", err));
        });
    }
});