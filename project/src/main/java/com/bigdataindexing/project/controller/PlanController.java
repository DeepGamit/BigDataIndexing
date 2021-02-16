package com.bigdataindexing.project.controller;


import com.bigdataindexing.project.exception.PlanAlreadyPresentException;
import com.bigdataindexing.project.exception.PlanNotFoundException;
import com.bigdataindexing.project.service.PlanService;
import org.json.JSONTokener;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;
import org.json.JSONObject;

import javax.servlet.http.HttpServletResponse;
import java.net.URI;
import java.net.URISyntaxException;


@RestController
public class PlanController {

    PlanService planService = new PlanService();


    @RequestMapping(method = RequestMethod.POST, produces = MediaType.APPLICATION_JSON_VALUE, value = "/plan")
    public ResponseEntity createPlan(@RequestBody String jsonData) throws URISyntaxException {

        JSONObject jsonPlan = new JSONObject(new JSONTokener(jsonData));
        this.planService.validatePlan(jsonPlan);

        if(this.planService.checkIfPlanExists((String) jsonPlan.get("objectId"))){
            throw new PlanAlreadyPresentException("Plan has already present!!");
        }

        String objectID = this.planService.savePlan(jsonPlan, (String)jsonPlan.get("objectType"));

        String message = "Plan Created Successfully!!";
        String responseBody = "{\n" +
                            "\t\"objectId\": \"" + objectID + "\"\n" +
                            "\t\"message\": \"" + message + "\"\n" +
                            "}";
        return ResponseEntity.created(new URI(jsonPlan.get("objectId").toString())).body(responseBody);
    }

    @RequestMapping(method = RequestMethod.GET, produces = MediaType.APPLICATION_JSON_VALUE, value = "/plan/{objectID}")
    public ResponseEntity getPlan(@PathVariable String objectID){

        if(!this.planService.checkIfPlanExists(objectID)){
            throw new PlanNotFoundException("Plan not found!!");
        }

        JSONObject jsonObject = this.planService.getPlan(objectID);

        return ResponseEntity.ok().body(jsonObject.toString());
    }

    @RequestMapping(method =  RequestMethod.DELETE, produces = MediaType.APPLICATION_JSON_VALUE, value = "/plan/{objectID}")
    public ResponseEntity deletePlan(@PathVariable String objectID){

        if(!this.planService.deletePlan(objectID)){
            throw new PlanNotFoundException("Plan not found!!");
        }

        return ResponseEntity.noContent().build();
    }

}
