from email.policy import HTTP
from django.shortcuts import render
from django.contrib.auth import authenticate, login


# Create your views here.

def index(request):
  
  iframe = ""
  return render(request, 'Dashboard.html', context)

def home(request):
  iframe = ""
  context = {'iframe': iframe}
  return render(request, 'home.html', context)


def user_login(request):
    if request.method == 'POST':
        # Process the request if posted data are available
        username = request.POST['username']
        password = request.POST['password']
        # Check username and password combination if correct
        user = authenticate(username=username, password=password)
        if user is not None:
            # Save session as cookie to login the user
            login(request, user)
            # Success, now let's login the user.
            return render(request, 'home.html')
        else:
            # Incorrect credentials, let's throw an error to the screen.
            return render(request, 'login.html', {'error_message': 'Incorrect username and / or password.'})
    else:
        # No post data availabe, let's just show the page to the user.
        return render(request, 'login.html')