package eu.nyuu.courses.rest;

import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
public class Controller {

    @GetMapping("/blabla")
    public String blablabla(){
        return "blabla";
    }


}
