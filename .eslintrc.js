module.exports = {
  "extends": "google",
  "env": {
    "es6": true
  },
  "rules": {
    "generator-star-spacing": [
      "error", {
        "before": true,
        "after": false
      }],
    "comma-dangle": 0,
    "require-jsdoc": 0,
    "no-extra-parens": 2,
    "arrow-parens": ["error", "as-needed"],
    "yield-star-spacing": ["error", {"before": true, "after": false}],
    "max-len": ["error", {
      "ignoreComments": true,
      "ignoreStrings": true,
      "ignoreTemplateLiterals": true
    }],
    "no-var": 0,
    "default-case": 0
  }
};
