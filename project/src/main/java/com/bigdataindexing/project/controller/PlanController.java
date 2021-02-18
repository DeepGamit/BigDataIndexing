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

        if(this.planService.checkIfKeyExists("plan" + "_" + (String) jsonPlan.get("objectId"))){
            return ResponseEntity.status(HttpStatus.CONFLICT)
                    .body(new JSONObject().put("message", "Plan already exists!!").toString());
        }

        String objectID = this.planService.savePlan(jsonPlan, (String)jsonPlan.get("objectType"));

        String message = "Plan Created Successfully!!";
        String responseBody = "{\n" +
                            "\t\"objectId\": \"" + objectID + "\"\n" +
                            "\t\"message\": \"" + message + "\"\n" +
                            "}";
        return ResponseEntity.created(new URI(jsonPlan.get("objectId").toString())).body(responseBody);
    }

    @RequestMapping(method = RequestMethod.GET, produces = MediaType.APPLICATION_JSON_VALUE, value = "/{type}/{objectID}")
    public ResponseEntity getPlan(@PathVariable String type, @PathVariable String objectID){

        if(!this.planService.checkIfKeyExists(type + "_" + objectID)){
            return ResponseEntity.status(HttpStatus.NOT_FOUND)
                    .body(new JSONObject().put("message", "ObjectId does not exists!!").toString());
        }

        JSONObject jsonObject = this.planService.getPlan(type + "_" + objectID);

        return ResponseEntity.ok().body(jsonObject.toString());
    }

    @RequestMapping(method =  RequestMethod.DELETE, produces = MediaType.APPLICATION_JSON_VALUE, value = "/plan/{objectID}")
    public ResponseEntity deletePlan(@PathVariable String objectID){

        if(!this.planService.checkIfKeyExists("plan" + "_" + objectID)){
            return ResponseEntity.status(HttpStatus.NOT_FOUND)
                    .body(new JSONObject().put("message", "ObjectId does not exists!!").toString());
        }

        if(!this.planService.deletePlan("plan" + "_"  + objectID)){
            return ResponseEntity.status(HttpStatus.BAD_REQUEST)
                    .body(new JSONObject().put("message", "Some error was encountered while deleting!!").toString());
        } else {
            return ResponseEntity.noContent().build();
        }
    }
}
