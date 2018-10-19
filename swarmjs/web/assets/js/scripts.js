
jQuery(document).ready(function() {
	
    /*
        Fullscreen background
    */
    //$.backstretch("http://fastml.com/images/wallpapers/new4/equations.jpg");

    $.backstretch("http://itknowledgeexchange.techtarget.com/mobile-cloud-view/files/2013/11/canstockphoto13148727.jpg");

    /*
        Form validation
    */
    $('.login-form input[type="text"], .login-form input[type="password"], .login-form textarea').on('focus', function() {
    	$(this).removeClass('input-error');
    });
    
    $('.login-form').on('submit', function(e) {
    	
    	$(this).find('input[type="text"], input[type="password"], textarea').each(function(){
    		if( $(this).val() == "" ) {
    			e.preventDefault();
    			$(this).addClass('input-error');
    		}
    		else {
    			$(this).removeClass('input-error');
    		}
    	});
    	
    });
    
    
});

$("#probe").click(function(){
	console.log("hello");
	var oldText = $("#joblog").val();
	$("#joblog").val(oldText+"\n>> job started ");
});