document.getElementById('checkAuth').addEventListener('change', function() {
    var form = document.getElementById('form_login');
    var user = document.getElementById('User');
    var password = document.getElementById('Password');
    var inputs = form.getElementsByTagName('input');
    for (var i = 0; i < inputs.length; i++) {
        if (inputs[i].type !== 'checkbox') {
            user.disabled = this.checked;
            password.disabled = this.checked;

        }
    }
});