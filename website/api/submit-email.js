const express = require("express");
const bodyParser = require("body-parser");
const Airtable = require("airtable");
require("dotenv").config();

const app = express();
app.use(bodyParser.json());

const base = new Airtable({ apiKey: process.env.AIRTABLE_API_KEY }).base(process.env.AIRTABLE_BASE_ID);
const table = base("RVSP");

app.post("/api/submit-email", async (req, res) => {
    const { email } = req.body;

    if (!email) {
        return res.status(400).json({ message: "Email is required." });
    }

    try {
        const records = await table
            .select({
                filterByFormula: `{Emails} = "${email}"`,
            })
            .firstPage();

        if (records.length > 0) {
            return res.status(200).json({ exists: true });
        }

        await table.create([{ fields: { Emails: email } }]);

        return res.status(201).json({ exists: false });
    } catch (error) {
        console.error("Error interacting with Airtable:", error);
        res.status(500).json({ message: "Failed to process the email. Please try again." });
    }
});

module.exports = app;