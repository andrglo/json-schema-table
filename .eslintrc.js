module.exports = {
  extends: 'google',
  parserOptions: {
    ecmaVersion: 2017,
    ecmaFeatures: {
      experimentalObjectRestSpread: true
    }
  },
  env: {
    node: true,
    es6: true,
    mocha: true
  },
  rules: {
    'generator-star-spacing': [
      'error',
      {
        before: true,
        after: false
      }
    ],
    'comma-dangle': 0,
    semi: ['error', 'never'],
    'require-jsdoc': 0,
    'valid-jsdoc': 0,
    'new-cap': 0,
    'quote-props': 0,
    'no-extra-parens': 0,
    'arrow-parens': ['error', 'as-needed'],
    'yield-star-spacing': ['error', {before: true, after: false}],
    'max-len': [
      'error',
      {
        ignoreComments: true,
        ignoreStrings: true,
        ignoreTemplateLiterals: true
      }
    ],
    'no-var': 0,
    camelcase: 0,
    'no-invalid-this': 0,
    'no-undef': 2,
    'no-unreachable': 2
  }
}
