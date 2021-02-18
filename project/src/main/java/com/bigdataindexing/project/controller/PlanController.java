package com.bigdataindexing.project.controller;


import com.bigdataindexing.project.service.PlanService;
import org.json.JSONTokener;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;
import org.json.JSONObject;

import java.net.URI;
import java.net.URISyntaxException;


@RestController
public class PlanController {

    PlanService planService = new PlanService();


    @RequestMapping(method = RequestMethod.POST, produces = MediaType.APPLICATION_JSON_VALUE, value = "/plan")
    public ResponseEntity createPlan(@RequestBody String jsonData) throws URISyntaxException {

        JSONObject jsonPlan = new JSONObject(new JSONTokener(jsonData));
        this.planService.validatePlan(jsonPlan);

        if(this.planService.checkIfKeyExists((String) jsonPlan.get("objectId"))){
            return ResponseEntity.status(HttpStatus.CONFLICT)
                    .body(new JSONObject().put("message", "Plan already exists!!").toString());
        }

        String objectID = this.planService.savePlan(jsonPlan);

        String message = "Plan Created Successfully!!";
        String responseBody = "{\n" +
                            "\t\"objectId\": \"" + objectID + "\"\n" +
                            "\t\"message\": \"" + message + "\"\n" +
                            "}";
        return ResponseEntity.created(new URI(jsonPlan.get("objectId").toString())).body(responseBody);
    }

    @RequestMapping(method = RequestMethod.GET, produces = MediaType.APPLICATION_JSON_VALUE, value = "/plan/{objectID}")
    public ResponseEntity getPlan(@PathVariable String objectID){

        if(!this.planService.checkIfKeyExists(objectID)){
            return ResponseEntity.status(HttpStatus.NOT_FOUND)
                    .body(new JSONObject().put("message", "ObjectId does not exists!!").toString());
        }

        JSONObject jsonObject = this.planService.getPlan( objectID);
        return ResponseEntity.ok().body(jsonObject.toString());
    }

    @RequestMapping(method =  RequestMethod.DELETE, produces = MediaType.APPLICATION_JSON_VALUE, value = "/plan/{objectID}")
    public ResponseEntity deletePlan(@PathVariable String objectID){

        if(!this.planService.checkIfKeyExists(objectID)){
            return ResponseEntity.status(HttpStatus.NOT_FOUND)
                    .body(new JSONObject().put("message", "ObjectId does not exists!!").toString());
        }

        this.planService.deletePlan(objectID);
        return ResponseEntity.noContent().build();
    }
}
